SELECT 
    regex_struct(
        /(?<user>[^ ]+)\s+(?<pid>[^ ]+)\s+(?<command>[^ ]+)/,
        col_0) as user
FROM RUN('ps axo user:20,pid,comm') as ps