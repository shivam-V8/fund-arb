FROM python:3.12-slim

WORKDIR /app

# App needs zero third-party packages — just the two source files.
COPY fund_arb.py fund_dashboard.html ./

# Bind to all interfaces so Docker's published port reaches the server.
ENV HOST=0.0.0.0 \
    PORT=8787

EXPOSE 8787

CMD ["python", "fund_arb.py", "--serve"]
