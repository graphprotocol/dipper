//! Reading replies from indexers. An indexer picks its own URL and runs its own server, so a
//! reply can be any length, or never end. Each is read only up to a limit, so that no indexer
//! can make dipper hold an unbounded body in memory.

/// Why a reply's body wasn't read.
#[derive(Debug, thiserror::Error)]
pub(super) enum ReplyError {
    /// The body is longer than the caller allows.
    #[error("the reply is longer than the limit of {limit} bytes")]
    TooLong { limit: usize },
    /// The connection failed or timed out before the body ended.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

/// Read a reply's body, giving up as soon as it is known to be longer than `limit` bytes: at
/// once when its declared length says so, otherwise once that many bytes have arrived.
pub(super) async fn read_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, ReplyError> {
    let declared_too_long = response
        .content_length()
        .is_some_and(|declared| usize::try_from(declared).map_or(true, |len| len > limit));
    if declared_too_long {
        return Err(ReplyError::TooLong { limit });
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(ReplyError::TooLong { limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::*;

    async fn get(url: &str) -> reqwest::Response {
        reqwest::Client::new().get(url).send().await.unwrap()
    }

    #[tokio::test]
    async fn a_body_up_to_the_limit_is_read_whole() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("12345"))
            .mount(&server)
            .await;

        let body = read_body(get(&server.uri()).await, 5).await.unwrap();

        assert_eq!(body, b"12345");
    }

    #[tokio::test]
    async fn a_body_declared_longer_than_the_limit_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("123456"))
            .mount(&server)
            .await;

        let refused = read_body(get(&server.uri()).await, 5).await.unwrap_err();

        assert!(
            matches!(refused, ReplyError::TooLong { limit: 5 }),
            "{refused}"
        );
    }

    /// A server that declares no length and never stops sending is the case a declared length
    /// can't catch: without a limit on the bytes that arrive, the read would go on until the
    /// request timed out, holding everything sent so far.
    #[tokio::test]
    async fn a_body_that_never_ends_is_refused_once_it_passes_the_limit() {
        //* Given a server that streams 1,024 bytes at a time for as long as anyone listens
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let head = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            let chunk = format!("400\r\n{}\r\n", "x".repeat(1024));
            while socket.write_all(chunk.as_bytes()).await.is_ok() {}
        });
        let response = get(&format!("http://{address}/")).await;

        //* When
        let refused = tokio::time::timeout(Duration::from_secs(5), read_body(response, 64 * 1024))
            .await
            .expect("the read must stop at the limit, not run until a timeout");

        //* Then
        assert!(
            matches!(refused, Err(ReplyError::TooLong { limit: 65_536 })),
            "{refused:?}"
        );
    }
}
