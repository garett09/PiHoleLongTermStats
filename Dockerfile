FROM python:3.12-slim-bookworm

# Install Unbound (for unbound-control)
RUN apt-get update && apt-get install -y unbound && rm -rf /var/lib/apt/lists/*

# Change the working directory to the `app` directory
WORKDIR /app

# copy required files
COPY pyproject.toml .
COPY LICENSE .
COPY piholelongtermstats ./piholelongtermstats

# install using pip
RUN pip install --no-cache-dir .

# Run the app
CMD ["piholelongtermstats"]