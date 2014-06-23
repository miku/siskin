_tsk_names()
{
    local cur=${COMP_WORDS[COMP_CWORD]}
    COMPREPLY=( $(compgen -W "$(tasknames)" -- $cur) )
}

complete -F _tsk_names taskcat
complete -F _tsk_names taskdo
complete -F _tsk_names taskless
complete -F _tsk_names taskoutput
