. |
{
	"finc.format":			"ElectronicArticle",
#	"finc.mega_collection":	"FID #5486",
	"finc.mega_collection":	["MedienwRezensionen"],
#	"finc.record_id":		["finc-73-",.id]|add,
	"finc.record_id": .front."article-meta"."article-id" | map(select(."@pub-id-type" == "doi")) | .[]."#text",
	"finc.id":		["finc-73-",  # type other or doi
		(.front."article-meta"."article-id" | map(select(."@pub-id-type" == "other")) | .[]."#text" )] | add,
    "doi": .front."article-meta"."article-id" | map(select(."@pub-id-type" == "doi")) | .[]."#text" ,
	"finc.source_id":		"73",
#	"authors":				[{"rft.au":.creator?}],
#	"authors":				[{"rft.au":.creator?|add}],
#	"authors":				.creator?|{reduce .[]} ,
#	"authors":				[.creator[]|{"rft.au":.}],
	"authors":	[
		{"rft.au":
		.front."article-meta"."contrib-group".contrib | map(select(."@contrib-type" == "author")) | .[].name | [."given-names", " ", ."surname"]|add
		}
	],
#	"url":					[.link],
	"url":					[.front."article-meta"."self-uri"[]|."@xlink:href"], # or just first [0] # with [] creates multiple records
    "rft.atitle":			.front."article-meta"."title-group"."article-title",
#	"rft.date":				([.dateissued,"-01-01"]|add),
#	"rft.date":				.front."article-meta"."pub-date" | map(select(."@pub-type" == "epub")) | .[] | [.year,"-",.month,"-",.day]|add,
	"rft.date":				.front."article-meta"."pub-date" | map(select(."@pub-type" == "collection")) | .[] | [.year]|add,
#    "x.date":           	.front."article-meta"."pub-date" | map(select(."@pub-type" == "epub")) | .[] | [.year,"-",.month,"-",.day , "T00:00:00Z"]|add,
    "x.date":           	.front."article-meta"."pub-date" | map(select(."@pub-type" == "collection")) | .[] | [.year,"-","01","-","01" , "T00:00:00Z"]|add, # no month/day in oai date/collection
#	"rft.pub":				[.front."journal-meta".publisher."publisher-name"],
#	"rft.place":			[.placeofpublication?],
	"rft.pub":	[.front."journal-meta".publisher."publisher-name" | split(", ")[0]],
	"rft.place":[.front."journal-meta".publisher."publisher-name" | split(", ")[-1] | gsub("(^[^a-z]+ )|([^a-z]+$)";"";"i") ],
	"rft.series":			.front."journal-meta"."journal-title",
#	"rft.jtitle":			.notes, # goes into solr:series, goes into In: # strip prefix:
#	"rft.jtitle":			(.notes|sub("Erschienen in: ";"")?),
	"rft.jtitle":	.front."journal-meta"."journal-title",
	"rft.issue":	.front."article-meta"."issue"."#text",
	"ris.type":				"ELEC",
#	"rft.genre":			"unknown",
#	"x.subjects":			[
	"issn": [.front."journal-meta".issn[]|."#text"], #both p/e issn,
	"abstract":				.front."article-meta".abstract.p,

	"version": "0.9"
}



# grep "10.17192/ep2011.2.271" extracted/73/73.json | jq .metadata.article -cr | jq -f bin/73/filter.jq
