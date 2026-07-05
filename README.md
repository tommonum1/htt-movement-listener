# HTT Movement Listener v2

Render settings:

Build command:

```bash
npm install
```

Start command:

```bash
npm start
```

Environment variables:

```text
NR_HOST=publicdatafeeds.networkrail.co.uk
NR_PORT=61618
NR_USERNAME=your Network Rail username
NR_PASSWORD=your Network Rail password
NR_TOPIC=/topic/TRAIN_MVT_ALL_TOC
```

Test endpoints:

- `/health`
- `/recent`
- `/active`
- `/latest/516T61D05`
- `/latest?ids=516T61D05,B29140,6T61`
- `/match?ids=516T61D05,B29140,6T61`
