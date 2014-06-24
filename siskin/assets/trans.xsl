<?xml version="1.0"?>

<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:m="http://www.loc.gov/MARC21/slim"
    xmlns:zs="http://www.loc.gov/zing/srw/"
    version="1.0">

    <xsl:output indent="yes" />
    
    <xsl:template match="/">
    <collection>
        <xsl:copy-of select="//m:record" />
    </collection>
    </xsl:template>

</xsl:stylesheet>

