<?xml version="1.0" encoding="UTF-8"?>
<!-- 

rvk.xml to breadcrumbs, i.e. path to root 

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

@author   Finc Team
@license  http://opensource.org/licenses/gpl-3.0.html GNU General Public License
@link     http://finc.info

-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  
  <xsl:output method='text'/>
  
  <xsl:variable name='newline'><xsl:text>&#10;</xsl:text></xsl:variable>
  <xsl:variable name='quot'><xsl:text>"</xsl:text></xsl:variable>
  
  <xsl:template match="/classification_scheme">
    <xsl:apply-templates select="node"/>
  </xsl:template>
 
  <xsl:template match="node">
    <xsl:apply-templates select="children"/>
    <xsl:for-each select="ancestor-or-self::node">
      <xsl:value-of select="$quot" /><xsl:value-of select="@notation"/><xsl:value-of select="$quot" />
      <xsl:if test="position() != last()">
        <xsl:text> &gt; </xsl:text>
      </xsl:if>  
    </xsl:for-each>    
    <xsl:value-of select="$newline" />
  </xsl:template>

  <xsl:template match="children">
    <xsl:apply-templates select="node"/>
  </xsl:template>
 
</xsl:stylesheet>

<!-- 
  <classification_scheme root_name="RVKO">
  <node notation = "A" benennung = "Allgemeines">
   <children>
    <node notation = "AA" benennung = "Bibliographien der Bibliographien, Universalbibliographien, Bibliothekskataloge, Nationalbibliographien">
     <children>
      <node notation = "AA 09900" benennung = "Bibliographische Zeitschriften">
       <content bemerkung ="(alphabetisch, Individualsignaturen)">
       </content>
       <register>Bibliographie / Zeitschrift
       </register>
      </node>
      <node notation = "AA 10000 - AA 19900" benennung = "Bibliographien der Bibliographien">
       <content bemerkung ="(aber nicht Bibliographien der Länder- und Fachbibliographien)">
       </content>
       <register>Bibliographie / Bibliographie
       </register>
       <children>
        <node notation = "AA 10000" benennung = "International, Allgemeines">
        </node>
        <node notation = "AA 10100" benennung = "Antike Welt">
        </node>
        <node notation = "AA 10300" benennung = "Ländergruppen/Entwicklungsländer">
        </node>
        <node notation = "AA 10400" benennung = "Westliche Welt, Abendland, Europäische Welt">
         <children>
          <node notation = "AA 10500" benennung = "Germanischer Kulturkreis">
          </node>
         </children>
        </node>
        <node notation = "AA 10600" benennung = "Europa">
         <children>
          <node notation 
-->