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

var Base64={_keyStr:"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",encode:function(e){var t="";var n,r,i,s,o,u,a;var f=0;e=Base64._utf8_encode(e);while(f<e.length){n=e.charCodeAt(f++);r=e.charCodeAt(f++);i=e.charCodeAt(f++);s=n>>2;o=(n&3)<<4|r>>4;u=(r&15)<<2|i>>6;a=i&63;if(isNaN(r)){u=a=64}else if(isNaN(i)){a=64}t=t+this._keyStr.charAt(s)+this._keyStr.charAt(o)+this._keyStr.charAt(u)+this._keyStr.charAt(a)}return t},decode:function(e){var t="";var n,r,i;var s,o,u,a;var f=0;e=e.replace(/[^A-Za-z0-9\+\/\=]/g,"");while(f<e.length){s=this._keyStr.indexOf(e.charAt(f++));o=this._keyStr.indexOf(e.charAt(f++));u=this._keyStr.indexOf(e.charAt(f++));a=this._keyStr.indexOf(e.charAt(f++));n=s<<2|o>>4;r=(o&15)<<4|u>>2;i=(u&3)<<6|a;t=t+String.fromCharCode(n);if(u!=64){t=t+String.fromCharCode(r)}if(a!=64){t=t+String.fromCharCode(i)}}t=Base64._utf8_decode(t);return t},_utf8_encode:function(e){e=e.replace(/\r\n/g,"\n");var t="";for(var n=0;n<e.length;n++){var r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r)}else if(r>127&&r<2048){t+=String.fromCharCode(r>>6|192);t+=String.fromCharCode(r&63|128)}else{t+=String.fromCharCode(r>>12|224);t+=String.fromCharCode(r>>6&63|128);t+=String.fromCharCode(r&63|128)}}return t},_utf8_decode:function(e){var t="";var n=0;var r=c1=c2=0;while(n<e.length){r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r);n++}else if(r>191&&r<224){c2=e.charCodeAt(n+1);t+=String.fromCharCode((r&31)<<6|c2&63);n+=2}else{c2=e.charCodeAt(n+1);c3=e.charCodeAt(n+2);t+=String.fromCharCode((r&15)<<12|(c2&63)<<6|c3&63);n+=3}}return t}}

var obj = JSON.parse(input);

var doc = {
    "id": "ai049" + Base64.encode(obj["URL"]),
    "source_id": "049",
    "url": obj["URL"],
    "record_id": obj["DOI"],
    "publisher": obj["publisher"],
    "issn": obj["ISSN"]
    // TODO: find or create suitable index fields
    // "journalVolume" : obj["volume"],
    // "journalIssue" : obj["issue"],
    // "journalPage" : obj["page"],
}

// only one type for now
if ("type" in obj && obj["type"] == "journal-article") {
    doc["format"] = "ElectronicArticle";
}

// assume only one container-title
if ("container-title" in obj) {
    doc["hierarchy_parent_title"] = obj["container-title"][0];
}

// all authors go into "author2" for now
var authors = [];
if ("author" in obj) {
    for (var i = 0; i < obj["author"].length; i++) {
        authors.push(obj["author"][i]["family"] + ", " + obj["author"][i]["given"]);
    }
}

doc["author2"] = authors;

// topic is an array
doc["topic"] = obj["subject"];

// address https://issues.apache.org/jira/browse/SOLR-6626
var pds = obj["issued"]["date-parts"][0][0];
if (pds != null) {
    doc["publishDateSort"] = obj["issued"]["date-parts"][0][0];
} else {
    doc["publishDateSort"] = "";
}

// collect dates
var dates = [];
for (var i = 0; i < obj["issued"]["date-parts"].length; i++) {
    dates.push(obj["issued"]["date-parts"][i].join(", "));
}
doc["publishDate"] = dates;

// title and subtitle
if ("subtitle" in obj && obj["subtitle"].length > 0) {
    doc["title"] = obj["title"][0] + " : " + obj["subtitle"][0];
} else {
    doc["title"] = obj["title"][0];
}
doc["title_full"] = (obj["title"].join(" ") + obj["subtitle"].join(" ")).trim()
doc["title_short"] = obj["title"][0]

// add catch-all field
var fields = [];
fields = fields.concat(doc["author2"]);
if ("topic" in doc) {
    fields = fields.concat(doc["topic"]);
}
if ("issn" in doc) {
    fields = fields.concat(doc["issn"]);
}
var other = [
    doc["title"],
    doc["publisher"],
    doc["hierarchy_parent_title"],
    doc["record_id"],
    doc["publishDateSort"]
];
fields = fields.concat(other);

doc["allfields"] = fields.join(" ")

// serialize
output = JSON.stringify(doc);
