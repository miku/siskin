SELECT 
	Reference.ID as ReferenceID,
	ReferenceType,
	Reference.Title as Title,
	SubTitle,
	GROUP_CONCAT(substr(COALESCE(LastName, '') || ', ' || COALESCE(FirstName, '') || ' ' || COALESCE(MiddleName, ''), 0), ';') as Person,
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
	Signatur
FROM	
	Reference
LEFT JOIN
	ReferenceAuthor ON (ReferenceAuthor.ReferenceID = Reference.ID)
LEFT JOIN
	Person ON (Person.ID = ReferenceAuthor.PersonID)
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
	GROUP BY ReferenceID) AS Location2 ON (Location2.ReferenceID = Reference.ID) 
GROUP BY
	Reference.ID