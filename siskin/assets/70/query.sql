SELECT 
    Reference.ID as ReferenceID,
    ReferenceType,
    Reference.Title as Title,
    SubTitle,
    Periodical.Name as Journal,
    Publisher.Name as Publisher,
    Language,
    Volume,
    Number as Issue,
    CustomField4 as Place,
    CustomField3 as Year,
    StartPage,
    EndPage,
    ISBN,
    Signatur,
    GROUP_CONCAT(Author , "; " ) As Author,
    GROUP_CONCAT(Editor , "; " ) As Editor,
    SeriesTitle.Name as Series
FROM
    Reference 
LEFT JOIN
    ReferenceAuthor ON (ReferenceAuthor.ReferenceID = Reference.ID)
LEFT JOIN
    (SELECT
        ID as AuthorID,
        substr(COALESCE(LastName, '') || ', ' || COALESCE(FirstName, '') || ' ' || COALESCE(MiddleName, ''), 0) as Author
    FROM
        Person) ON (AuthorID = ReferenceAuthor.PersonID)
LEFT JOIN
    ReferenceEditor ON (ReferenceEditor.ReferenceID = Reference.ID)
LEFT JOIN
    (SELECT
        ID as EditorID,
        substr(COALESCE(LastName, '') || ', ' || COALESCE(FirstName, '') || ' ' || COALESCE(MiddleName, ''), 0) as Editor
    FROM
        Person) ON (EditorID = ReferenceEditor.PersonID)
LEFT JOIN
    ReferencePublisher ON (ReferencePublisher.ReferenceID = Reference.ID)
LEFT JOIN
    Publisher ON (Publisher.ID = ReferencePublisher.PublisherID)
LEFT JOIN
    Periodical ON (Periodical.ID = Reference.PeriodicalID)
LEFT JOIN
    (SELECT
        GROUP_CONCAT(CallNumber , "; " ) AS Signatur,
        ReferenceID
    FROM
        Location
    GROUP BY
        ReferenceID) as Location2 ON (Location2.ReferenceID = Reference.ID)
LEFT JOIN
    SeriesTitle ON (SeriesTitle.ID = Reference.SeriesTitleID)
WHERE
	ReferenceType != "Contribution" AND
	Title NOT NULL
GROUP BY
    Reference.ID