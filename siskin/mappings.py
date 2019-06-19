# coding: utf-8
# pylint: disable=C0103,W0232,C0301,W0703

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
#					Robert Schenk, <robert.schenk@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>

"""
Static mappings for reuse
"""

from collections import defaultdict

formats = defaultdict(dict)

formats["Unknown"]["Leader"] = "     xxx  22        4500"
formats["Unknown"]["007"] = "tu"
formats["Unknown"]["008"] = ""
formats["Unknown"]["336b"] = ""
formats["Unknown"]["338b"] = ""
formats["Unknown"]["655a"] = ""
formats["Unknown"]["6552"] = ""
formats["Unknown"]["935c"] = ""

formats["Multipart"]["Leader"] = "     cam  22       a4500"
formats["Multipart"]["007"] = "tu"
formats["Multipart"]["008"] = ""
formats["Multipart"]["336b"] = ""
formats["Multipart"]["338b"] = ""
formats["Multipart"]["655a"] = ""
formats["Multipart"]["6552"] = ""
formats["Multipart"]["935c"] = ""

formats["Manuscript"]["Leader"] = "     ctm  22        4500"
formats["Manuscript"]["007"] = "tu"
formats["Manuscript"]["008"] = ""
formats["Manuscript"]["336b"] = ""
formats["Manuscript"]["338b"] = ""
formats["Manuscript"]["655a"] = ""
formats["Manuscript"]["6552"] = ""
formats["Manuscript"]["935c"] = ""

formats["Thesis"]["Leader"] = "     cam  22        4500"
formats["Thesis"]["007"] = "tu"
formats["Thesis"]["008"] = ""
formats["Thesis"]["336b"] = ""
formats["Thesis"]["338b"] = ""
formats["Thesis"]["655a"] = ""
formats["Thesis"]["6552"] = ""
formats["Thesis"]["935c"] = ""

formats["E-Thesis"]["Leader"] = "     cam  22        4500"
formats["E-Thesis"]["007"] = "cr"
formats["E-Thesis"]["008"] = ""
formats["E-Thesis"]["336b"] = ""
formats["E-Thesis"]["338b"] = ""
formats["E-Thesis"]["655a"] = ""
formats["E-Thesis"]["6552"] = ""
formats["E-Thesis"]["935c"] = ""

formats["Score"]["Leader"] = "     cam  22        4500"
formats["Score"]["007"] = "tu"
formats["Score"]["008"] = ""
formats["Score"]["336b"] = ""
formats["Score"]["338b"] = ""
formats["Score"]["655a"] = ""
formats["Score"]["6552"] = ""
formats["Score"]["935c"] = "muno"

formats["E-Score"]["Leader"] = "     cam  22        4500"
formats["E-Score"]["007"] = "cr"
formats["E-Score"]["008"] = ""
formats["E-Score"]["336b"] = ""
formats["E-Score"]["338b"] = ""
formats["E-Score"]["655a"] = ""
formats["E-Score"]["6552"] = ""
formats["E-Score"]["935c"] = "muno"

formats["Book"]["Leader"] = "     cam  22        4500"
formats["Book"]["007"] = "tu"
formats["Book"]["008"] = ""
formats["Book"]["336b"] = ""
formats["Book"]["338b"] = ""
formats["Book"]["655a"] = ""
formats["Book"]["6552"] = ""
formats["Book"]["935c"] = ""

formats["E-Book"]["Leader"] = "     cam  22        4500"
formats["E-Book"]["007"] = "cr"
formats["E-Book"]["008"] = ""
formats["E-Book"]["336b"] = ""
formats["E-Book"]["338b"] = ""
formats["E-Book"]["655a"] = ""
formats["E-Book"]["6552"] = ""
formats["E-Book"]["935c"] = ""

formats["Series"]["Leader"] = "     cas  22        4500"
formats["Series"]["007"] = "tu"
formats["Series"]["008"] = ""
formats["Series"]["336b"] = ""
formats["Series"]["338b"] = ""
formats["Series"]["655a"] = ""
formats["Series"]["6552"] = ""
formats["Series"]["935c"] = ""

formats["Newspaper"]["Leader"] = "     cas  22        4500"
formats["Newspaper"]["007"] = "tu"
formats["Newspaper"]["008"] = "                     n"
formats["Newspaper"]["336b"] = ""
formats["Newspaper"]["338b"] = ""
formats["Newspaper"]["655a"] = ""
formats["Newspaper"]["6552"] = ""
formats["Newspaper"]["935c"] = "zt"

formats["Journal"]["Leader"] = "     cas  22        4500"
formats["Journal"]["007"] = "tu"
formats["Journal"]["008"] = "                     p"
formats["Journal"]["336b"] = ""
formats["Journal"]["338b"] = ""
formats["Journal"]["655a"] = ""
formats["Journal"]["6552"] = ""
formats["Journal"]["935c"] = ""

formats["E-Journal"]["Leader"] = "     cas  22        4500"
formats["E-Journal"]["007"] = "tu"
formats["E-Journal"]["008"] = ""
formats["E-Journal"]["336b"] = ""
formats["E-Journal"]["338b"] = ""
formats["E-Journal"]["655a"] = ""
formats["E-Journal"]["6552"] = ""
formats["E-Journal"]["935c"] = ""

formats["Article"]["Leader"] = "     cab  22        4500"
formats["Article"]["007"] = "tu"
formats["Article"]["008"] = ""
formats["Article"]["336b"] = ""
formats["Article"]["338b"] = ""
formats["Article"]["655a"] = ""
formats["Article"]["6552"] = ""
formats["Article"]["935c"] = ""

formats["E-Article"]["Leader"] = "     cab  22        4500"
formats["E-Article"]["007"] = "cr"
formats["E-Article"]["008"] = ""
formats["E-Article"]["336b"] = ""
formats["E-Article"]["338b"] = ""
formats["E-Article"]["655a"] = ""
formats["E-Article"]["6552"] = ""
formats["E-Article"]["935c"] = ""

formats["Chapter"]["Leader"] = "     caa  22        4500"
formats["Chapter"]["007"] = "tu"
formats["Chapter"]["008"] = ""
formats["Chapter"]["336b"] = ""
formats["Chapter"]["338b"] = ""
formats["Chapter"]["655a"] = ""
formats["Chapter"]["6552"] = ""
formats["Chapter"]["935c"] = ""

formats["E-Chapter"]["Leader"] = "     caa  22        4500"
formats["E-Chapter"]["007"] = "cr"
formats["E-Chapter"]["008"] = ""
formats["E-Chapter"]["336b"] = ""
formats["E-Chapter"]["338b"] = ""
formats["E-Chapter"]["655a"] = ""
formats["E-Chapter"]["6552"] = ""
formats["E-Chapter"]["935c"] = ""

formats["Loose-leaf"]["Leader"] = ""
formats["Loose-leaf"]["007"] = "tu"
formats["Loose-leaf"]["008"] = ""
formats["Loose-leaf"]["336b"] = ""
formats["Loose-leaf"]["338b"] = ""
formats["Loose-leaf"]["655a"] = ""
formats["Loose-leaf"]["6552"] = ""
formats["Loose-leaf"]["935c"] = "lo"

formats["DVD-Video"]["Leader"] = ""
formats["DVD-Video"]["007"] = ""
formats["DVD-Video"]["008"] = ""
formats["DVD-Video"]["336b"] = ""
formats["DVD-Video"]["338b"] = ""
formats["DVD-Video"]["655a"] = ""
formats["DVD-Video"]["6552"] = ""
formats["DVD-Video"]["935c"] = "vide"

formats["DVD-Audio"]["Leader"] = ""
formats["DVD-Audio"]["007"] = ""
formats["DVD-Audio"]["008"] = ""
formats["DVD-Audio"]["336b"] = ""
formats["DVD-Audio"]["338b"] = ""
formats["DVD-Audio"]["655a"] = ""
formats["DVD-Audio"]["6552"] = ""
formats["DVD-Audio"]["935c"] = ""

formats["DVD-ROM"]["Leader"] = ""
formats["DVD-ROM"]["007"] = ""
formats["DVD-ROM"]["008"] = ""
formats["DVD-ROM"]["336b"] = ""
formats["DVD-ROM"]["338b"] = ""
formats["DVD-ROM"]["655a"] = ""
formats["DVD-ROM"]["6552"] = ""
formats["DVD-ROM"]["935c"] = ""

formats["CD-Video"]["Leader"] = ""
formats["CD-Video"]["007"] = ""
formats["CD-Video"]["008"] = ""
formats["CD-Video"]["336b"] = ""
formats["CD-Video"]["338b"] = ""
formats["CD-Video"]["655a"] = ""
formats["CD-Video"]["6552"] = ""
formats["CD-Video"]["935c"] = "vide"

formats["CD-Audio"]["Leader"] = ""
formats["CD-Audio"]["007"] = ""
formats["CD-Audio"]["008"] = ""
formats["CD-Audio"]["336b"] = ""
formats["CD-Audio"]["338b"] = ""
formats["CD-Audio"]["655a"] = ""
formats["CD-Audio"]["6552"] = ""
formats["CD-Audio"]["935c"] = ""

formats["CD-ROM"]["Leader"] = ""
formats["CD-ROM"]["007"] = "co"
formats["CD-ROM"]["008"] = ""
formats["CD-ROM"]["336b"] = ""
formats["CD-ROM"]["338b"] = ""
formats["CD-ROM"]["655a"] = ""
formats["CD-ROM"]["6552"] = ""
formats["CD-ROM"]["935c"] = ""

formats["Floppy-Disk"]["Leader"] = ""
formats["Floppy-Disk"]["007"] = "cj"
formats["Floppy-Disk"]["008"] = ""
formats["Floppy-Disk"]["336b"] = ""
formats["Floppy-Disk"]["338b"] = ""
formats["Floppy-Disk"]["655a"] = ""
formats["Floppy-Disk"]["6552"] = ""
formats["Floppy-Disk"]["935c"] = ""

formats["Object"]["Leader"] = ""
formats["Object"]["007"] = ""
formats["Object"]["008"] = ""
formats["Object"]["336b"] = ""
formats["Object"]["338b"] = "nr"
formats["Object"]["655a"] = ""
formats["Object"]["6552"] = ""
formats["Object"]["935c"] = ""


