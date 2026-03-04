#!/bin/bash

# Bash completion for the unified `siskin` command.
# Source this file or place it in /etc/bash_completion.d/.

_siskin()
{
    local cur prev
    cur=${COMP_WORDS[COMP_CWORD]}
    prev=${COMP_WORDS[COMP_CWORD-1]}

    # Subcommands that accept a task name as first argument.
    local task_cmds="run inspect output deps deps-dot cleanup cat help rm dir du head less ls open redo status tree wc"

    # All subcommands.
    local subcommands="run names inspect output config deps deps-dot docs cleanup hash ps home importcache checksetup version tags cat help rm dir du gc head less ls open redo status tree wc"

    # Complete subcommand at position 1.
    if [[ $COMP_CWORD -eq 1 ]]; then
        COMPREPLY=( $(compgen -W "$subcommands" -- "$cur") )
        return
    fi

    # Complete task names for subcommands that accept them.
    if [[ $COMP_CWORD -eq 2 ]]; then
        local subcmd=${COMP_WORDS[1]}
        for cmd in $task_cmds; do
            if [[ "$subcmd" == "$cmd" ]]; then
                local names
                names=$(siskin names 2>/dev/null)
                COMPREPLY=( $(compgen -W "$names" -- "$cur") )
                return
            fi
        done
    fi
}

complete -F _siskin siskin
