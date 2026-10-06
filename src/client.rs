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
            HttpClientError::UnexpectedStatusCode(status, _) => status.is_server_error(),
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

    /// Set the request timeout (the default is 5 seconds).
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
