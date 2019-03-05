
## Esql
a ligthwight tool of elasticsearch of go (depend ES6.0). inspired from [gorm](https://github.com/jinzhu/gorm), an ORM database tool.

Elasticearch api support json format, and fruitful api with it's specially format to support various search function,
very flexible and extendable. But we are difficult to organize so many level conditions manually. 

Usually, we prefer to know what the function of this api and how to use it, but not very care how it works.

Esql try to give big convenience on this issues. it wants help you just to concentrate on conditions of API, and 
easy to set conditions.

    e.g. definition: Match(i ...Setting)
    conditions actually are settings of fields, one field one setting. it allows you set many conditons same time. 
   
    1. Match(esql.F{"field":"setting"})
    
    2. Match(esql.F{"field":"setting"},esql.F{"field2":"setting2"},esql.F{"field3":"setting3"}) 
    

### Overview
* Index 
    * IndexExists    
    * CreateIndex 
    * Delete
    * ShowMapping
    * AutoMapping
    
* Document crud
    * GetDocWithID
    * IndexDoc
    * UpdateDoc
    * DeleteDoc
    * UpdatePartialDoc
    * AutoIndexDocs
     
* Search
    * Dismax
    * Where as Match
    * Not as MustNot
    * Or as Should
    * In as Term
    * Missing
    * NotNil
    * Between
    * Order
    * Limit
    * StringQuery
    * SimpleStringSelect
    * Phrase
    * Must
    * MustNot
    * Should
    * Filter
    * Range
    * Term
    * Terms
    * Regexp
    * Fuzzy
    * Wildcard
    * Scroll
    * GetScroll
    * Joins
    * MatchAll
    
* Aggregation
    * Bucket
        * Group as GroupTerms
        * GroupTerms
        * GroupDateHistogram
        * GroupHistogram
        * GroupDateRange
        * GroupIPRange
        * GroupRange
        * GroupGeoDistance
    * Metric (all)
        * Avg
        * Max
        * Sum
        * Count
        * Stats
        * WeightedAvg
        * Cardinality
        * ExtendedStats
        * GeoBounds
        * GeoCentroid
        * Percentiles
        * PercentileRanks

### Esql tool

* ###### NewElasticSearch: a convenient client tool for glboal.
it allows you assign a table(index) at first, the following options always work on it.
```go   
    es:=esql.NewElasticSearch("esql,product,user"); 
    var s1,s2,s3 []sturct
    go func(){
        es.DB().Where(esql.F{"name":"name"}).Limit(10).Find(&s1)
    }()
    
    go func(){
        es.DB().Term(esql.F{"gender":"make"}).Limit(50,100).Count("gender").Find(&s1)
    }()
    
    go func(){
        es.DB().Range(esql.F{"age":esql.F{"gt":18}).Find(&s1)
    }()
```
* ###### Condition tool(F & Not):
 ideally, you just concentrate on conditions of Match. if you have multi conditions, should make F slice.

    __ F __: a alias of map, name form `Find` and a positive action. 
    
    __ Not __: a alias of map, indicate a negative action. if you use it in __ any __ searching api, it will auto as  __MustNot__ condition.
    
    
### Esql cases:
```go
  es:=esql.NewElasticSearch("esql")
  
  var aggregation struct
  var results []struct
  if err:=es.DB().Where(esq.F{"name":"input my name"},esq.F{"age":18}).
        Match(esql.F{"content":"input the text"}).
        Not(esql.Not{"name":"do want"}).
        Or(esql.F{"should":"maybeok"}).
        Order(esql.F{"name":"desc"}).
        Limit(5).
        Find(&results).
        Error;err!=nil{
            log.Println(err.Error())
    }
  
  if err:=es.DB().Term(esql.F{"age":18,"gender":"male"},esql.F{"language_code":"en-gb"}).
        Range(esql.F{"created_at":esql.F{"gt":"2015-01-01","lte":"2019-01-01"}}).
        Group("color").
        Sum("group_color").
        Count("age").
        Find(&aggregation).
        Error;err!=nil{
            log.Println(err.Error())
    }
       
```

