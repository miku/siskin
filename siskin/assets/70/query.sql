SELECT 
            ReferenceID,
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
			GROUP_CONCAT(CallNumber , "; " ) as Signatur
        FROM
			Reference
		LEFT JOIN ReferenceAuthor USING (ReferenceID)
		LEFT JOIN Person USING (PersonID)
		LEFT JOIN ReferencePublisher USING (ReferenceID)
		LEFT JOIN Publisher USING (PublisherID)
		LEFT JOIN Periodical USING (PeriodicalID)
		LEFT JOIN Location USING (ReferenceID)		
		GROUP BY
			Reference.ReferenceID