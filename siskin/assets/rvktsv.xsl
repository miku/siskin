<!-- Convert RVK XML into (notation, name) TSV. -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  
  <xsl:output method='text'/>
  
  <xsl:variable name='newline'><xsl:text>&#10;</xsl:text></xsl:variable>
  <xsl:variable name='tab'><xsl:text>&#x9;</xsl:text></xsl:variable>
  <xsl:variable name='quot'><xsl:text>"</xsl:text></xsl:variable>
  
  <xsl:template match="/classification_scheme">
    <xsl:apply-templates select="//node"/>
  </xsl:template>
 
  <xsl:template match="node">
    <xsl:value-of select="@notation" />
    <xsl:value-of select="$tab" />
    <xsl:value-of select="@benennung" />
    <xsl:value-of select="$newline" />
  </xsl:template>
 
</xsl:stylesheet>
