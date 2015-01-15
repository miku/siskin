// {
//    "prefix" : "http://id.crossref.org/prefix/10.1111",
//    "type" : "journal-article",
//    "volume" : "44",
//    "deposited" : {
//       "date-parts" : [
//          [
//             2007,
//             2,
//             13
//          ]
//       ],
//       "timestamp" : 1171324800000
//    },
//    "source" : "CrossRef",
//    "author" : [
//       {
//          "given" : "Phyllis A.",
//          "family" : "Katz"
//       },
//       {
//          "given" : "Carol",
//          "family" : "Seavey"
//       }
//    ],
//    "score" : 1,
//    "page" : "770-775",
//    "subject" : [
//       "Developmental and Educational Psychology",
//       "Education",
//       "Pediatrics, Perinatology, and Child Health"
//    ],
//    "title" : [
//       "Labels and Children's Perception of Faces"
//    ],
//    "publisher" : "Wiley-Blackwell",
//    "member" : "http://id.crossref.org/member/311",
//    "ISSN" : [
//       "0009-3920",
//       "1467-8624"
//    ],
//    "indexed" : {
//       "date-parts" : [
//          [
//             2014,
//             5,
//             12
//          ]
//       ],
//       "timestamp" : 1399876413297
//    },
//    "issued" : {
//       "date-parts" : [
//          [
//             1973,
//             12
//          ]
//       ]
//    },
//    "subtitle" : [],
//    "URL" : "http://dx.doi.org/10.1111/j.1467-8624.1973.tb01150.x",
//    "issue" : "4",
//    "container-title" : [
//       "Child Development"
//    ],
//    "reference-count" : 18,
//    "DOI" : "10.1111/j.1467-8624.1973.tb01150.x"
// }


var obj = JSON.parse(input);
var doc = {}

doc["url"] = obj["URL"];
doc["record_id"] = obj["DOI"];
doc["publisher"] = obj["publisher"];
doc["journalVolume"] = obj["volume"];
doc["journalIssue"] = obj["issue"];
doc["journalPage"] = obj["page"];

if (obj["type"] == "journal-article") {
    doc["format"] = "ElectronicArticle";
}

if ("container-title" in obj) {
    doc["hierarchy_parent_title"] = obj["container-title"][0];
}

var authors = [];
if ("author" in obj) {
    for (var i = 0; i < obj["author"].length; i++) {
        authors.push(obj["author"][i]["family"] + ", " + obj["author"][i]["given"]);
    }
}
doc["author2"] = authors;
doc["topic"] = obj["subject"];
doc["publishDateSort"] = obj["issued"]["date-parts"][0][0];

var dates = [];
for (var i = 0; i < obj["issued"]["date-parts"].length; i++) {
    dates.push(obj["issued"]["date-parts"][i].join(", "));
}
doc["publishDate"] = dates;

doc["issn"] = obj["ISSN"];
if ("subtitle" in obj && obj["subtitle"].length > 0) {
    doc["title"] = obj["title"][0] + " : " + obj["subtitle"][0];
} else {
    doc["title"] = obj["title"][0];
}

output = JSON.stringify(doc);