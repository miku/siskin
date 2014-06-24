<?xml version="1.0"?>

<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:m="http://www.loc.gov/MARC21/slim"
    xmlns:z="http://www.indexdata.dk/zebra/"
    version="1.0">

    <xsl:output indent="yes" />

    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
    
    <xsl:template match="//z:idzebra" />

</xsl:stylesheet>
