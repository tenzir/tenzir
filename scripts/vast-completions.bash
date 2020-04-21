#!/usr/bin/env bash

_vast_completions()
{
  completion_words=("${COMP_WORDS[@]}")
  if [ "${COMP_WORDS[COMP_CWORD]}" == "--" ]; then
    # We need to special-case '--', since that has special
    # meaning, e.g.
    #
    #    vast pivot --asdf help -> displays error message/help text
    #    vast pivot -- help     -> runs pivot command
    #
    completion_words[$((COMP_CWORD))]="-"
  fi
  if [[ "${completion_words[@]}" =~ " -- " ]]; then
    # The command line had a '--' not under the current cursor;
    # do nothing in this case.
    COMPREPLY=()
    return
  fi
  SAVEIFS=$IFS
  IFS=$'\n'
  commands=($(${completion_words[@]} help 2>&1 |  awk '/subcommands:/{x = 1} !/subcommands:/{if (x > 0) print $1} match($0, /--[^\]]*]/) { print substr($0, RSTART, RLENGTH-1) }'))
  IFS=$SAVEIFS
  COMPREPLY=($(compgen -W "$(echo ${commands[@]})" -- ${COMP_WORDS[$COMP_CWORD]}))
}

complete -F _vast_completions vast
