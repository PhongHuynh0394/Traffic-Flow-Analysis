FROM jupyter/minimal-notebook:python-3.9

USER root

# RUN apt-get update && apt-get install -y \
#     apt-transport-https \
#     ca-certificates \
#     curl \
#     gnupg && \
#     curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg && \
#     echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=$(dpkg --print-architecture)] https://packages.clickhouse.com/deb stable main" > /etc/apt/sources.list.d/clickhouse.list && \
#     apt-get update && apt-get install -y clickhouse-client && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*


WORKDIR /home/jovyan

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN playwright install

WORKDIR /home/jovyan/workplace