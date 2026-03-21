# Use stable Python (NOT 3.13)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run bot
CMD ["python", "bot.py"]
