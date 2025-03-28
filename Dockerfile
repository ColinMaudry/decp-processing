FROM python:3.12

# Set the working directory
WORKDIR /app

# Install Rust and Cargo
RUN apt-get update && apt-get install -y curl
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install .

# Run main.py when the container launches
CMD ["python", "src/flows.py"]