# IMBA Beacon Deployment

## Current Stack

- Frontend deployed separately on Netlify
- Backend deployed separately on Render
- Backend entry point: `server.js`
- Waitlist storage: `data/waitlist.json`
- Payments storage: `data/payments.json`

## Important Note

The current backend stores waitlist submissions and payment records in local JSON files.

This is fine for local development and quick demos, but on most cloud hosts this file storage is **not persistent** across rebuilds, restarts, or redeploys.

For a real production deployment, the next step should be moving waitlist storage to:

- SQLite
- PostgreSQL
- MongoDB

## Required Environment Variables

Create these variables on Render:

- `GOOGLE_CLIENT_ID`
- `PAYTM_MID`
- `PAYTM_MERCHANT_KEY`
- `PAYTM_WEBSITE`
- `PAYTM_ENV`
- `APP_BASE_URL`
- `FRONTEND_ORIGIN`

For local development, copy `.env.example` values into your own environment loader or export them manually before starting the app.

## Recommended Deployment Split

- Netlify: frontend
- Render: backend API

## Render Deployment

This project already includes `render.yaml`.

### Steps

1. Push this project to GitHub.
2. Log in to Render.
3. Create a new Web Service from the GitHub repo.
4. Render should detect:
   - Build Command: `npm install`
   - Start Command: `npm start`
5. Deploy.

### Result

Your backend will be available at a URL like:

`https://imba-beacon.onrender.com`

## Netlify Deployment

This project already includes `netlify.toml`.

### Steps

1. Push this project to GitHub.
2. Create a new site in Netlify from the GitHub repo.
3. Netlify will publish the root folder using `index.html`.
4. After deployment, open `site-config.js` and replace:

```js
window.IMBA_SITE_CONFIG = {
  apiBaseUrl: "http://127.0.0.1:3000"
};
```

with your Render backend URL:

```js
window.IMBA_SITE_CONFIG = {
  apiBaseUrl: "https://imba-beacon.onrender.com"
};
```

5. Redeploy Netlify after that change.

## GoDaddy DNS

Recommended setup:

- `www.yourdomain.com` -> Netlify frontend
- `api.yourdomain.com` -> Render backend (optional later)

If you keep the backend on the Render default URL, then:

- point your main site domain to Netlify
- keep the API on Render

## Local Run

```bash
npm install
npm start
```

Then open:

`http://127.0.0.1:3000`
