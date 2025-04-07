# Start with a lightweight base image
FROM alpine:latest

# Install SQLite
RUN apk add --no-cache sqlite

# Set the defaut working directory
WORKDIR /db

# Set the defaut command to open SQLite
CMD ["sqlite3"]