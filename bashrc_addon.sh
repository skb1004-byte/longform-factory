
# autopus-adk: ? ????? ?? init
autopus_check() {
  if [ -f "CLAUDE.md" ] && [ ! -f "autopus.yaml" ]; then
    echo "?? autopus-adk: ??? ?..."
    /home/song/.local/bin/auto init --yes 2>/dev/null && echo "? ??"
  fi
}
# cd ? ??? ??
cd() { builtin cd "$@" && autopus_check; }
# git clone ? ?? ??
gitclone() { git clone "$@" && cd "$(basename "${1%.git}")" ; }
