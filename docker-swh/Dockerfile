FROM debian:bookworm

# Set environment variables for non-interactive installations
ENV DEBIAN_FRONTEND=noninteractive

# Create a user and set up sudoers
RUN useradd -m user && echo 'user ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Set the working directory as root
WORKDIR /home/

# Install necessary packages as root
RUN apt-get update && apt-get install -y \
    build-essential \
    libclang-dev \
    python3 \
    python3-venv \
    python3-pip \
    default-jre \
    zstd \
    protobuf-compiler \
    curl \
    pkg-config \
    git \
    libssl-dev \
    libpq-dev \
    python3-dev \
    vim \
    && apt-get clean

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Set up Rust environment variables
ENV PATH="/root/.cargo/bin:${PATH}"

# Install swh-graph-grpc-server
RUN RUSTFLAGS="-C target-cpu=native" cargo install --git https://gitlab.softwareheritage.org/swh/devel/swh-graph.git swh-graph-grpc-server

RUN python3 -m venv .venv

RUN . .venv/bin/activate && \
    pip install psycopg2-binary google grpcio chardet awscli pandas boto3 pyarrow \
    datatrove tokenizers xxhash orjson nltk \
    transformers datasketch datasets typer wheel torch chromadb

# Clone the repository and build the Rust project
RUN . .venv/bin/activate && \
    git clone https://gitlab.softwareheritage.org/swh/devel/swh-graph.git && \
    cd swh-graph && \
    cargo build --release -p swh-graph-grpc-server && \
    pip3 install .

# Install swh-graph
RUN cargo install swh-graph

WORKDIR "/home/swh-graph"
RUN cargo build --release --bin swh-graph-grpc-serve

WORKDIR /home/

RUN . .venv/bin/activate && \
    git clone https://github.com/ASUS-AICS/LSH.git && \
    cd LSH && \
    pip install Cython && \
    python setup.py install

RUN . .venv/bin/activate && \
    python -c "import nltk; nltk.download('punkt_tab')"

RUN echo "alias sv='source /home/.venv/bin/activate'" >> ~/.bashrc

RUN . .venv/bin/activate && \
    pip install accelerate termcolor
