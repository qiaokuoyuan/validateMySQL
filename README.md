# validateMySQL
cache &amp; validate table  structure for MySQL


```mermaid
graph LR
    

cache --> file

file --> result 
```

## cache
```bash
validate-mysql -c 
```

- `-o` : target file
- `-P` : mysql database port
- `-H` : mysql database host
- `-d` : mysql database schema name

## validate 
```bash
validate-mysql -v
```
- `-o` : target file where the validate result saved to(.xlsx format), 
- `-i` : input database info file, which is the output file above
- `-P` : mysql database port
- `-H` : mysql database host
- `-d` : mysql database schema name
