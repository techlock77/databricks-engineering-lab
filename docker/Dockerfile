FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl unzip git ca-certificates && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

WORKDIR /dbx-demo-lab

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash"]
