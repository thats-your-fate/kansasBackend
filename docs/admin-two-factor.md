# Kansas private admin authentication

Kansas admin authentication is implemented by the sibling `kansasMarketing`
Next server, not this public data API. It now requires the owner-provisioned
shared **SEO Sites** authenticator entry after password/CAPTCHA. No new API
login route, database, key or runtime identity belongs in this backend.

Deploy the API and web together through the existing native Git release helper.
The web's encrypted state file remains private and locked under fw-ks-runtime;
its credentials, MFA encryption key and shared-seed transfer stay outside Git.
See kansasMarketing/docs/admin-two-factor.md and the ops shared-authenticator
rollout matrix. A backend-only deployment does not enable private admin MFA.
