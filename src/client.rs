use std::{
    fmt::{self, Debug, Display, Formatter},
    time::Duration,
};

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

pub use reqwest::{header, Method, StatusCode, Url};

use crate::error::Error;

/// Log message send error.
pub struct SendError {
    inner: Error,
    can_retry: bool,
}

impl SendError {
    /// Check if the send request can be retried.
    #[inline]
    pub fn can_retry(&self) -> bool {
        self.can_retry
    }
}

impl Debug for SendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Display for SendError {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl std::error::Error for SendError {
    #[inline]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.inner)
    }
}

impl From<SendError> for Error {
    #[inline]
    fn from(err: SendError) -> Self {
        err.inner
    }
}

/// Cloud service client.
#[trait_variant::make(Send)]
pub trait Client {
    type Message;

    /// Send a given log message to the cloud service.
    async fn send(&self, msg: Self::Message) -> Result<(), SendError>;
}

/// HTTP client error.
#[derive(Debug)]
pub enum HttpClientError {
    UnexpectedStatusCode(StatusCode, Bytes),
    Other(Error),
}

impl Display for HttpClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedStatusCode(status, body) => {
                write!(f, "server responded with HTTP {status}")?;

                if f.alternate() {
                    write!(f, ":\n{}", String::from_utf8_lossy(body.as_ref()))?;
                }

                Ok(())
            }
            Self::Other(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for HttpClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnexpectedStatusCode(_, _) => None,
            Self::Other(err) => Some(err),
        }
    }
}

impl From<Error> for HttpClientError {
    #[inline]
    fn from(err: Error) -> Self {
        Self::Other(err)
    }
}

impl From<HttpClientError> for Error {
    fn from(err: HttpClientError) -> Self {
        match err {
            HttpClientError::Other(err) => err,
            err => Error::from_cause(err),
        }
    }
}

impl From<HttpClientError> for SendError {
    fn from(err: HttpClientError) -> Self {
        let can_retry = match err {
            HttpClientError::UnexpectedStatusCode(status, _) => {
                let code = status.as_u16();

                code == 408 || code == 429 || code >= 500
            }
            HttpClientError::Other(_) => true,
        };

        Self {
            inner: err.into(),
            can_retry,
        }
    }
}

/// HTTP client builder.
pub struct HttpClientBuilder {
    method: Method,
    headers: HeaderMap<HeaderValue>,
    request_timeout: Duration,
}

impl HttpClientBuilder {
    /// Create a new builder.
    fn new() -> Self {
        Self {
            method: Method::POST,
            headers: HeaderMap::new(),
            request_timeout: Duration::from_secs(60),
        }
    }

    /// Set the request method.
    #[inline]
    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    /// Set a give request header.
    pub fn header<K, V>(mut self, key: K, value: V) -> Result<Self, Error>
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let key: HeaderName = key
            .try_into()
            .map_err(|err| Error::from_static_msg_and_cause("invalid header name", err))?;

        let value: HeaderValue = value
            .try_into()
            .map_err(|err| Error::from_static_msg_and_cause("invalid header value", err))?;

        self.headers.insert(key, value);

        Ok(self)
    }

    /// Set the request headers.
    #[inline]
    pub fn headers(mut self, headers: HeaderMap<HeaderValue>) -> Self {
        self.headers = headers;
        self
    }

    /// Set the request timeout (the default is 60 seconds).
    #[inline]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Create the HTTP client.
    pub fn build(self, url: Url) -> Result<HttpClient, Error> {
        let client = reqwest::Client::builder()
            .timeout(self.request_timeout)
            .build()
            .map_err(|err| {
                Error::from_static_msg_and_cause("unable to create an HTTP client", err)
            })?;

        let res = HttpClient {
            url,
            method: self.method,
            headers: self.headers,
            client,
        };

        Ok(res)
    }
}

/// HTTP client.
pub struct HttpClient {
    url: Url,
    method: Method,
    headers: HeaderMap<HeaderValue>,
    client: reqwest::Client,
}

impl HttpClient {
    /// Get an HTTP client builder.
    #[inline]
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::new()
    }

    /// Send a given message.
    pub async fn send(&self, body: Bytes) -> Result<(), HttpClientError> {
        let method = self.method.clone();
        let url = self.url.clone();
        let headers = self.headers.clone();

        let response = self
            .client
            .request(method, url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|err| Error::from_static_msg_and_cause("unable to send a request", err))?;

        let status = response.status();

        let body = response.bytes().await.map_err(|err| {
            Error::from_static_msg_and_cause("unable to read a response body", err)
        })?;

        if status.is_success() {
            Ok(())
        } else {
            Err(HttpClientError::UnexpectedStatusCode(status, body))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    };

    use crate::error::Error;

    use super::{HttpClient, HttpClientError, Method, SendError, StatusCode, Url};

    /// Accept a single HTTP request and respond with a given status code and
    /// body.
    ///
    /// The returned task resolves to the request head and body.
    async fn serve_once(status: u16, body: &'static str) -> (Url, JoinHandle<(String, Vec<u8>)>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        let local_addr = listener.local_addr();

        let url = format!("http://{}/some/path", local_addr.unwrap())
            .parse()
            .unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            let mut request = Vec::new();

            let head_len = loop {
                read_more(&mut stream, &mut request).await;

                if let Some(pos) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };

            let head = std::str::from_utf8(&request[..head_len])
                .unwrap()
                .to_string();

            let content_length = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse().unwrap())
                .unwrap_or(0);

            while request.len() < head_len + content_length {
                read_more(&mut stream, &mut request).await;
            }

            let response = format!(
                "HTTP/1.1 {status} Status\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );

            stream.write_all(response.as_bytes()).await.unwrap();

            (head, request.split_off(head_len))
        });

        (url, server)
    }

    /// Read more data from a given stream into a given buffer.
    async fn read_more(stream: &mut TcpStream, buffer: &mut Vec<u8>) {
        let mut chunk = [0u8; 4096];

        let len = stream.read(&mut chunk).await.unwrap();

        assert!(len > 0, "connection closed");

        buffer.extend_from_slice(&chunk[..len]);
    }

    #[test]
    fn only_transient_errors_can_be_retried() {
        let cases = [
            (400, false),
            (401, false),
            (403, false),
            (404, false),
            (408, true),
            (413, false),
            (429, true),
            (500, true),
            (502, true),
            (503, true),
        ];

        for (code, expected) in cases {
            let status = StatusCode::from_u16(code).unwrap();

            let err = SendError::from(HttpClientError::UnexpectedStatusCode(status, Bytes::new()));

            assert_eq!(err.can_retry(), expected, "HTTP {code}");
        }

        let err = SendError::from(HttpClientError::Other(Error::from_static_msg(
            "connection reset",
        )));

        assert!(err.can_retry());
    }

    #[tokio::test]
    async fn http_client_sends_configured_request() {
        let (url, server) = serve_once(200, "").await;

        let client = HttpClient::builder()
            .method(Method::PUT)
            .header("X-Test", "value")
            .unwrap()
            .build(url)
            .unwrap();

        client.send(Bytes::from_static(b"payload")).await.unwrap();

        let (head, body) = server.await.unwrap();

        assert!(head.starts_with("PUT /some/path HTTP/1.1\r\n"), "{head}");
        assert!(
            head.to_ascii_lowercase().contains("\r\nx-test: value\r\n"),
            "{head}"
        );
        assert_eq!(body, b"payload");
    }

    #[tokio::test]
    async fn http_client_reports_unexpected_status() {
        let (url, server) = serve_once(503, "busy").await;

        let client = HttpClient::builder().build(url).unwrap();

        match client.send(Bytes::new()).await {
            Err(HttpClientError::UnexpectedStatusCode(status, body)) => {
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(body, "busy");
            }
            res => panic!("unexpected result: {res:?}"),
        }

        server.await.unwrap();
    }
}
