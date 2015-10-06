#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
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
#

# siskin task name completion
#
# TODO(miku): autocomplete task parameters, too
_siskin_task_names()
{
    hash tasknames 2>/dev/null || { return 1; }
    local cur=${COMP_WORDS[COMP_CWORD]}
    COMPREPLY=( $(compgen -W "$(tasknames)" -- $cur) )
}

complete -F _siskin_task_names taskcat
complete -F _siskin_task_names taskdeps-dot
complete -F _siskin_task_names taskdir
complete -F _siskin_task_names taskdo
complete -F _siskin_task_names taskdu
complete -F _siskin_task_names taskfast
complete -F _siskin_task_names taskhead
complete -F _siskin_task_names taskhelp
complete -F _siskin_task_names taskless
complete -F _siskin_task_names taskls
complete -F _siskin_task_names taskopen
complete -F _siskin_task_names taskoutput
complete -F _siskin_task_names taskredo
complete -F _siskin_task_names taskrm
complete -F _siskin_task_names taskstatus
complete -F _siskin_task_names tasktree
complete -F _siskin_task_names taskwc
