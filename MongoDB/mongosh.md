# Mongosh
## Database
```bash
show dbs
use school
db.createCollection("students")
db.dropDatabase()  # drop the current database
```

## Insert
```bash
db.students.insertOne({name: "Spongebob", age: 30, gpa: 3.2})
db.students.find()
db.students.insertMany([
    {name: "Patrick", age: 38, gpa: 1.5},
    {name: "Sandy", age: 27, gpa: 4.0},
    {name: "Gary", age: 18, gpa: 2.5},
])
db.students.insertOne(
    {
    name: "Larry",
    age: 32,
    gpa: 2.8,
    fullTime: false,
    resisterDate: new Date(),
    graduationDate: null
    courses: [
        "Biology", 
        "Chemistry", 
        "Calculus"
        ],
    address: {
        street: "123 Fake St.",
        city: "Bikini Bottom",
        zip: 12345
        }
    }
)
```

## Sort and Limit
```bash
db.students.find().sort({name: 1})  # 1 for alphabetical order
db.students.find().sort({gpa: -1})  # -1 for reverse alphabetical order
db.students.find().limit(3)
db.students.find().sort({gpa: -1}).limit(3)
```

## Find
```bash
db.students.find({name: "Spongebob"}, gpa: 3.2)
db.students.find({}, {name: true})  # .find({query}, {projection})
db.students.find({}, {_id: false, name: true, gpa: true})  # .find({query}, {projection})
```
## Update
```bash
db.students.updateOne({name: "Spongebob"}, {$set: {fullTime: true}})  # .updateOne({filter}, {update})
db.students.updateOne({name: "Spongebob"}, {$unset: {fullTime: ""}})  # remove a `fullTime` field
db.students.updateMany({}, {$set: {fullTime: false}})  # the `fullTime` field of every record will be updated to false
db.students.updateMany({fullTime: {$exists: false}}, {$set: {fullTime: true}})  # The record doesn't have a `fullTime` field will be updated to true
```

## Delete
```bash
db.students.deleteOne({name: "Larry"})
db.students.deleteMany({fullTime: false})
db.students.deleteMany({registerDate: {$exists: false}})  # delete every record doesn't have a `registerDate` field
```

## Comparison operators
```bash
db.students.find({name: {$ne: "Spongebob"}})
db.students.find({age: {$lt: 20}})
db.students.find({age: {$lte: 27}})
db.students.find({age: {$gt: 27}})
db.students.find({age: {$gte: 27}})
db.students.find({gpa: {$gte: 3, $lte: 4}})
db.students.find({name: {$in: ["Spongebob", "Patrick", "Sandy"]}})
db.students.find({name: {$nin: ["Spongebob", "Patrick", "Sandy"]}})
```

## Logical operators
```bash
db.students.find({$and: [{fullTime: true}, {age: {$lte: 22}}]})
db.students.find({$or: [{fullTime: true}, {age: {$lte: 22}}]})
db.students.find({$nor: [{fullTime: true}, {age: {$lte: 22}}]})
db.students.find({age: {$not: {$gte: 30}}})
```

## Indexes
```bash
db.students.find({name: "Larry"}).explain("executionStats")  # notice that docExamined: 5
db.students.createIndex({name: 1})  # index "name_1" will be created
db.students.find({name: "Larry"}).explain("executionStats") # notice that docExamined: 1 -> query faster!!!
db.students.getIndexes()
db.students.dropIndex("name_1")
```

## Collections
```bash
show collections
db.createCollection("teachers", {capped: true, size: 1024000, max: 100}, {autoIndexId: false})
db.createCollection("courses")
db.courses.drop()
```