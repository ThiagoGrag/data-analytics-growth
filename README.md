# Ingestão dos dados
Workspace -> Users -> thiagogdantis@gmail.com -> data-analytics-growth
Utilizei PySpark

## Pasta: bronze
Bases de order, consumer e restaurant subi utilizando notebooks e realizei o discovery incial nessa camada.
Não salvei dados nessa camada a não ser a base do teste_ab.

## Tabelas:
ab_test_ref = tabela com os dados do teste AB. O restante das tabelas não salvei no databricks.

# Modelagem dos dados
## Pasta: silver -> silver_order
Fiz o join com a tabela customer e test_ab

Verifiquei se tinha campo nulo e excluí 8.505 dados sem customer_id
Retirei os campos que tinha valores zerado do order_total_amount

Criei as segmentações para a região, delivery_time e minimum_order_value.

Analisando os dados notei que temos poucos clientes inativos, cerca de 8 mil somente. Não considerei

## Pasta: silver -> analise_descritiva

Fiz avaliações de resumo e notei dados duplicados order_id. 
Retirei as duplicações.

## Tabelas:
orders_enriched = tabela com os joins para unir a order, merchant, consumer e test_ab;

orders_time = tabela com os joins para unir a order, merchant, consumer e test_ab com o dado de data da criação do pedido;

orders_time_distinct = tabela sem as duplicações;

orders_time_distinct_class = tabela com os campos de classificação no delivery_time e minimum_order_value.

## Pasta: gold -> model_order
Reduzi a base para balancear as variáveis.
Apliquei o modelo K-prototype.
Voltei com os tipos dos dados que o spark permite.
Transformei a base em pandas para spark novamente e salvei as tabelas com cluster
Criei os subsets para realizar as análises estatísticas dos testes ab.
Após rodar o modelo de clusterização, criei o subset para rodar o modelo estatístico na base com os cluster.
Restante do código não utilizei.

## Pasta: gold -> ab_test
Teste t para os dados gerais do teste ab. Análise simples para verificar se os dados brutos eram estatisticamente diferentes. Utilizei os valores já calculados para rodar mais rápido.
Rodei o modelo ANOVA para cada variável que decidi analisar.
Plataforma de origem
Faixa de Preço
Valor mínimo
Tempo entrega
Região
Cluster

## Tabelas
abtest_cluster = tabela com a informação de cluster e dados para a avaliação estatística;

abtest_delivery_time = tabela com a informação de do tempo de entrega e dados para a avaliação estatística;

abtest_minium_value = tabela com a informação de valor mínimo e dados para a avaliação estatística;

abtest_platform = tabela com a informação de plataforma de origem e dados para a avaliação estatística;

abtest_price_range = tabela com a informação de faixa de preço e dados para a avaliação estatística;

abtest_region = tabela com a informação de região e dados para a avaliação estatística;

orders_model = tabela com a informação de pedidos pronta para o modelo, já selecionando as colunas que serão utilizadas no modelo;

orders_model = tabela com a informação de pedidos pronta para o modelo e análise do testeab, mantive alguns campos que vou utilizar nas análises;

orders_model_cluster_result = tabela final após rodar o modelo K-prototype com os clusters;

orders_segmentados = tabela somente com os clusters, mas sem as informações necessárias para rodar o teste ab.


# SQL Editor
Código básicos para verificações
# DASHBOARD
## order_analysis
Análise exploratória de todos os dados e entender comportamentos.
Divisão entre quantidade de pedidos e valores dos pedidos. Essa primeira visão não tinha excluído as duplicações ainda.

Entendimento de como o grupo target e control eram impactados.
Notei o desbalanceamento e depois ajustei esse ponto.

Decidi começar a analisar somente o ticket médio geral, ticket médio do cliente e a frequência de compra.

Decidi avaliar as 5 variáveis: Plataforma de origem; Faixa de Preço; Valor mínimo; Tempo entrega; Região


## ab_test_analysis
Com a base já balanceada e limpa, segui com a análise exploratória.
Avaliei todas as variáveis para entender se houve grandes mudanças.
Resolvi seguir analisado somente o ticket_medio e frequência

Verifiquei todas as variáveis pelo grupo do teste ab.

Após rodar o modelo do Cluster, avaliei os resultados também.

# Teste de Hipótese
1ª 
H0 = Não há diferença significativa entre o controle e o target no ticket médio por cliente
H1 = Há diferença significativa entre o controle e o target no ticket médio por cliente 

2ª 
H0 = Não há diferença significativa entre o controle e o target na frequência
H1 = Há diferença significativa entre o controle e o target na frequência 
