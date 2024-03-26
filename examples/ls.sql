SELECT 
    regex_struct(
        /(?:[^ ]+\s+){2}(?<user>[^ ]+)\s+(?:[^ ]+\s+)(?<size>[^ ]+)\s+(?:[^ ]+\s+){3}(?<filename>[^ ]+)/,
        col_0) as user
FROM RUN('ls -la') as ls