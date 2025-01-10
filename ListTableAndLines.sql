SELECT
    table_name AS tabela,
    table_rows AS quantidade_de_linhas
FROM
    information_schema.tables
WHERE
    table_schema = 'sistema_financeiro'
ORDER BY
    table_rows DESC;