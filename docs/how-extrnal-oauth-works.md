Cloud storages usually require OAuth (Google Drive, Dropbox, OneDrive)

OAuth services usually need HTTPS domain for authorization. Self hosted Postgresus can be hosted via HTTP or even without static IP so this way does not work. To make OAuth works even on localhost, we proxy requests via postgresus.com domain

As permanent URL for authorization we use main Postgresus domain. It forward responses to the self hosted domain so it can get access to the cloud

This is the sequence of requests (example for Google Drive):

```mermaid
sequenceDiagram
    participant SelfHosted as http://localhost:4005<br/>Self-hosted Postgresus
    participant Proxy as https://postgresus.com<br/>Proxy website
    participant Google as Google OAuth

    SelfHosted->>Google: Send auth request with DTO

    Google->>Proxy: Redirect with auth code<br/>to postgresus.com/oauth

    Proxy->>SelfHosted: Redirect to self-hosted instance<br/>with DTO + auth code

    SelfHosted->>Google: Exchange auth code for tokens<br/>POST /oauth2/token
    Google->>SelfHosted: Return access & refresh tokens

    SelfHosted->>SelfHosted: Store Google Drive config for files exchange
```

