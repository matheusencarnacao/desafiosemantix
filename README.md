<h1>Desafio Engenheiro de Dados</h1>

## Qual o objetivo do comando cache em Spark?
Armazenar os resultados obtidos em disco, já que o padrão é armazenar em memória, assim quando houver a necessidade de analisar muitas vezes o mesmo conjunto de dados, você pode reaproveita-lo em cache.

## O mesmo código implementado em Spark é normalmente mais rápido que a implementação
equivalente em MapReduce. Por quê?
Sim, pois o MapReduce realiza as operações por etapas e já o Spark realiza todas as operações no conjunto de dados de uma só vez.

## Qual é a função do SparkContext?
Cria uma conexão com o cluster do Spark onde será criado os RDD's.

## Explique com suas palavras o que é Resilient Distributed Datasets(RDD).
Estrutura de dados distribuidos em cluster utilizado pelo Spark para manipulação dos dados, podendo ser realizada de forma paralela.

## GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
Sim, pois o GroupByKey agrupa todas as chaves e já o reduceByKey, reaproveita a partição de chaves em comum, assim evita a transição de dados desnecessários.

## Explique o que o código Scala abaixo faz.
	
	val textFile = sc.textFile("hdfs://...")
	val counts = textFile.flatMap(line => line.split(""))
		.map(word => (word, 1))
		.reduceByKey(_ + _)
	counts.saveAsTextFile("hdfs://...")

Realiza uma contagem de palavras de um arquivo de Hadoop.
