<?php

/**
 *
 * proof-of-concept converter for Pig-style IMSLP data into MARCXML
 *
 * Copyright (C) 2012 Leander Seige, seige@ub.uni-leipzig.de
 * Leipzig University Library, Project finc
 * http://www.ub.uni-leipzig.de
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author   Leander Seige
 * @license  http://opensource.org/licenses/gpl-3.0.html GNU General Public License
 * @link     http://finc.info
 *
 */

    function put_record($in) {
        if (1>preg_match_all('/^(.*)\^\"?{(.*)\}"?$/',$in,$outa,PREG_SET_ORDER)) {
            # file_put_contents('php://stderr', ''.$in.'\n'); # we missed some 1000 lines
            return;
        }

        $id=htmlspecialchars($outa[0][1]);
        //$id=strtolower($id);
        //$id=preg_replace("/[^\w]+/","-",$id);

        preg_match_all("/([^,]+=[^=]+)(,|$)/",$outa[0][2],$outb,PREG_SET_ORDER);

        // print_r($outa);
        // print_r($outb);

        echo "<record>\n";
        echo "<leader>00000    a2200000   4500</leader>\n";
        echo "<controlfield tag=\"001\">".md5($id)."</controlfield>\n";

        foreach($outb as $ob) {
            // echo $ob[1]."\n";
            $pair=explode("=",trim($ob[1]));
            // csv escaped quotes don't get unescaped yet
            $valraw=str_replace('""','"',$pair[1]);
            $value=htmlspecialchars($valraw);
            $key=$pair[0];
            $set[$key]=$value;
        }

        if(isset($set['Year/Date of Composition'])) {
            $set['Year']=$set['Year/Date of Composition'];
        }

        foreach ($set as $k => $v){
            switch($k) {

                case "Work Title":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"245\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    if(isset($set["Opus/Catalogue Number"])) {
                        echo "    <subfield code=\"b\">".$set["Opus/Catalogue Number"]."</subfield>\n";
                    }
                    echo "  </datafield>\n";
                    break;

                case "Alternative Title":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"246\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                case "Composer":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"100\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                case "Key":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"384\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                /* Ticket #1682
                 * case "Movements/Sections":
                 *     echo "  <datafield ind1=\" \" ind2=\" \" tag=\"505\">\n";
                 *     echo "    <subfield code=\"a\">".$v."</subfield>\n";
                 *     echo "  </datafield>\n";
                *     break;
                */

                case "Year":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"650\">\n";
                    echo "    <subfield code=\"y\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                case "Librettist":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"700\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                case "Language":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"546\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                case "Dedication":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"500\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                case "Piece Style":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"590\">\n";
                    echo "    <subfield code=\"a\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                case "Instrumentation":
                    echo "  <datafield ind1=\" \" ind2=\" \" tag=\"590\">\n";
                    echo "    <subfield code=\"b\">".$v."</subfield>\n";
                    echo "  </datafield>\n";
                    break;

                default:
                    # file_put_contents('php://stderr', 'unknown key: '.$k);
                    break;
            }
        }
        echo "  <datafield ind1=\" \" ind2=\" \" tag=\"856\" >\n";
        echo "    <subfield code=\"u\">http://imslp.org/wiki/".rawurlencode($id)."</subfield>\n";
        echo "    <subfield code=\"3\">Petrucci-Musikbibliothek</subfield>\n";
        echo "  </datafield>\n";
        echo "  <datafield ind1=\" \" ind2=\" \" tag=\"970\" >\n";
        echo "    <subfield code=\"c\">PN</subfield>\n";
        echo "  </datafield>\n";
        echo "</record>\n";

    }

    if(!isset($argv[1])) {
        die("no filename specified\n");
    }

    $file=$argv[1];

    $handle = @fopen($file, "r");
    if ($handle) {
        echo "<collection>\n";
            while (($buffer = fgets($handle, 16384)) !== false) {
                put_record($buffer);
            }
        echo "</collection>\n";
        if (!feof($handle)) {
                echo "Error: unexpected fgets() fail\n";
            }
            fclose($handle);
    }

?>
