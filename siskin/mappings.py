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

formats["Book"]["Leader"] = ""
formats["Book"]["007"] = ""
formats["Book"]["008"] = ""
formats["Book"]["336b"] = ""
formats["Book"]["338b"] = ""
formats["Book"]["655a"] = ""
formats["Book"]["6552"] = ""
formats["Book"]["935c"] = ""

formats["E-Book"]["Leader"] = ""
formats["E-Book"]["007"] = ""
formats["E-Book"]["008"] = ""
formats["E-Book"]["336b"] = ""
formats["E-Book"]["338b"] = ""
formats["E-Book"]["655a"] = ""
formats["E-Book"]["6552"] = ""
formats["E-Book"]["935c"] = ""

formats["Article"]["Leader"] = ""
formats["Article"]["007"] = ""
formats["Article"]["008"] = ""
formats["Article"]["336b"] = ""
formats["Article"]["338b"] = ""
formats["Article"]["655a"] = ""
formats["Article"]["6552"] = ""
formats["Article"]["935c"] = ""

formats["E-Article"]["Leader"] = ""
formats["E-Article"]["007"] = ""
formats["E-Article"]["008"] = ""
formats["E-Article"]["336b"] = ""
formats["E-Article"]["338b"] = ""
formats["E-Article"]["655a"] = ""
formats["E-Article"]["6552"] = ""
formats["E-Article"]["935c"] = ""

formats["Chapter"]["Leader"] = ""
formats["Chapter"]["007"] = ""
formats["Chapter"]["008"] = ""
formats["Chapter"]["336b"] = ""
formats["Chapter"]["338b"] = ""
formats["Chapter"]["655a"] = ""
formats["Chapter"]["6552"] = ""
formats["Chapter"]["935c"] = ""

formats["E-Chapter"]["Leader"] = ""
formats["E-Chapter"]["007"] = ""
formats["E-Chapter"]["008"] = ""
formats["E-Chapter"]["336b"] = ""
formats["E-Chapter"]["338b"] = ""
formats["E-Chapter"]["655a"] = ""
formats["E-Chapter"]["6552"] = ""
formats["E-Chapter"]["935c"] = ""

formats["Loose-leaf"]["Leader"] = ""
formats["Loose-leaf"]["007"] = ""
formats["Loose-leaf"]["008"] = ""
formats["Loose-leaf"]["336b"] = ""
formats["Loose-leaf"]["338b"] = ""
formats["Loose-leaf"]["655a"] = ""
formats["Loose-leaf"]["6552"] = ""
formats["Loose-leaf"]["935c"] = ""

formats["DVD-Video"]["Leader"] = ""
formats["DVD-Video"]["007"] = ""
formats["DVD-Video"]["008"] = ""
formats["DVD-Video"]["336b"] = ""
formats["DVD-Video"]["338b"] = ""
formats["DVD-Video"]["655a"] = ""
formats["DVD-Video"]["6552"] = ""
formats["DVD-Video"]["935c"] = ""

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
formats["CD-Video"]["935c"] = ""

formats["CD-Audio"]["Leader"] = ""
formats["CD-Audio"]["007"] = ""
formats["CD-Audio"]["008"] = ""
formats["CD-Audio"]["336b"] = ""
formats["CD-Audio"]["338b"] = ""
formats["CD-Audio"]["655a"] = ""
formats["CD-Audio"]["6552"] = ""
formats["CD-Audio"]["935c"] = ""

formats["CD-ROM"]["Leader"] = ""
formats["CD-ROM"]["007"] = ""
formats["CD-ROM"]["008"] = ""
formats["CD-ROM"]["336b"] = ""
formats["CD-ROM"]["338b"] = ""
formats["CD-ROM"]["655a"] = ""
formats["CD-ROM"]["6552"] = ""
formats["CD-ROM"]["935c"] = ""





