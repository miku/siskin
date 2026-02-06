# siskin CLI aliases for backward compatibility.
#
# The `siskin` command is the single entry point; the old task* commands are
# mapped to `siskin <subcommand>`.
#
# Usage: source this file in your shell profile, e.g.
#
#     . /path/to/siskin_aliases.sh
#
# Install siskin with: uv tool install -U siskin
alias taskcat="siskin cat"
alias taskchecksetup="siskin checksetup"
alias taskcleanup="siskin cleanup"
alias taskconfig="siskin config"
alias taskdeps="siskin deps"
alias taskdeps-dot="siskin deps-dot"
alias taskdir="siskin dir"
alias taskdo="siskin run"
alias taskdocs="siskin docs"
alias taskdu="siskin du"
alias taskgc="siskin gc"
alias taskhash="siskin hash"
alias taskhead="siskin head"
alias taskhelp="siskin help"
alias taskhome="siskin home"
alias taskimportcache="siskin importcache"
alias taskinspect="siskin inspect"
alias taskless="siskin less"
alias taskls="siskin ls"
alias tasknames="siskin names"
alias taskopen="siskin open"
alias taskoutput="siskin output"
alias taskps="siskin ps"
alias taskredo="siskin redo"
alias taskrm="siskin rm"
alias taskstatus="siskin status"
alias tasksync="siskin sync"
alias tasktags="siskin tags"
alias tasktree="siskin tree"
alias taskversion="siskin version"
alias taskwc="siskin wc"
