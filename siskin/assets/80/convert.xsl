<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:template match="/">
    <list>
      <xsl:apply-templates select="//Resource"/>
    </list>
  </xsl:template>
  <xsl:template match="Resource">
    <resource>
      <xsl:attribute name="id">
        <xsl:value-of select="@id"/>
      </xsl:attribute>
      <id><xsl:value-of select="@id"/></id>
      <xsl:apply-templates select="Attribute"/>
    </resource>
  </xsl:template>
  <xsl:template match="Attribute">
    <xsl:for-each select="AttributeValue">
      <xsl:element name="{../@label}">
        <xsl:value-of select=".//AttributeComponent"/>
      </xsl:element>
    </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>
