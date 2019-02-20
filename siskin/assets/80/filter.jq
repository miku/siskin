. |
{
    "finc.format":          "ElectronicArticle",
#   "finc.mega_collection": "FID #5374",
    "finc.mega_collection": ["Datenbank Internetquellen"],
    "finc.record_id":       .id,
    "finc.id":              ["finc-80-",.id]|add,
    "finc.source_id":       "80",
#   "authors":              [{"rft.au":.creator?}],
#   "authors":              [{"rft.au":.creator?|add}],
#   "authors":              .creator?|{reduce .[]} ,
    "authors":              [.creator[]?|{"rft.au":.}],
    "url":                  [.link],
    "rft.atitle":           .title,
    # "rft.date":           .dateissued, # some have full date yyyy-mm-dd:
    "rft.date":
        # if (.name | length) > 0 then A else B end
        (if (.dateissued | length) == 10 then .dateissued elif (.dateissued | length) == 4 then ([.dateissued,"-01-01"]|add) else null end),
#   "rft.date":             ([.dateissued // "","-01-01"]|add),
#   "x.date":              ([.dateissued,"-01-01T00:00:00Z"]|add),
#   "x.date":               ([.dateissued // "","-01-01T00:00:00Z"]|add),
    "x.date":
        (if (.dateissued | length) == 10 then ([.dateissued,"T00:00:00Z"]|add) elif (.dateissued | length) == 4 then ([.dateissued,"-01-01T00:00:00Z"]|add) else null end),
#   "rft.pub":              [(select(.publisher != null)?], ## span error: array into string
    "rft.pub":              [.publisher[0]?],
    "rft.place":            [.placeofpublication?],
    "rft.series":           .notes,
#   "rft.stitle":           .notes,
#   "rft.jtitle":           .notes, # goes into solr:series, goes into In: # strip prefix:
    "rft.jtitle":           (.notes|sub("Erschienen in: ";"")?),

    "ris.type":             "ELEC",
    "rft.genre":            "unknown",
#	"x.subjects":           [.subjectfreeKMW + .subjectfreeTW + .subjectfreeFW],

    "abstract":             .description?,

    "version": "0.9"
}
