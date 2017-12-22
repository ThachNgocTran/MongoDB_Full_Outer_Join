db.distributor1.createIndex({global_id: 1})
db.distributor1.createIndex({category1: 1, category2: 1})
db.distributor2.createIndex({global_id: 1})
db.distributor2.createIndex({category1: 1, category2: 1})

// Get all ObjectId in distributor1, that belong to cat1_X/cat2_Y.
var col1 = []
db.distributor1.find({category1: "cat1_X", category2: "cat2_Y"}).forEach(function(doc){
    col1.push(doc._id)
})

// Get all ObjectId in distributor2, that belong to cat1_M/cat2_N.
var col2 = []
db.distributor2.find({category1: "cat1_M", category2: "cat2_N"}).forEach(function(doc){
    col2.push(doc._id)
})

// The target category1/category2 we want to merge to.
var targetCat1 = "cat1_target"
var targetCat2 = "cat2_target"

var merged = []

// Do: distributor1 LEFT OUTER JOIN distributor2.
for(var index = 0; index < col1.length; index++){
    var objId1 = col1[index]
    var doc1 = db.distributor1.findOne({_id: objId1})
    var newDoc = {}
    newDoc["global_id"] = doc1.global_id
    newDoc["category1"] = targetCat1
    newDoc["category2"] = targetCat2
    newDoc["distributor1"] = [doc1]
    newDoc["distributor2"] = []
    for(var index2 = 0; index2 < col2.length; index2++){
        var objId2 = col2[index2]
        var doc2 = db.distributor2.findOne({_id: objId2})
        if (doc2.global_id == doc1.global_id){
            newDoc["distributor2"] = [doc2]
            col2 = col2.filter(function(input){ return !input.equals(objId2)})
            break
        }
    }
    merged.push(newDoc)
}

// Get the remaining items in distributor2, so that we have FULL OUTER JOIN.
for(var index = 0; index < col2.length; index++){
    var objId2 = col2[index]
    var doc2 = db.distributor2.findOne({_id: objId2})
    var newDoc = {}
    newDoc["global_id"] = doc2.global_id
    newDoc["category1"] = targetCat1
    newDoc["category2"] = targetCat2
    newDoc["distributor1"] = []
    newDoc["distributor2"] = [doc2]
    merged.push(newDoc)
}

// Now the list "merged" is the result of FULL OUTER JOIN between cat1_X/cat2_Y in distributor1 and cat1_M/cat2_N in distributor2.
merged
