# Monitor Claude Code agent sessions

# Default: monitor any active Claude session (last 100 lines)
monitor:
    @osascript tools/monitor-agent.scpt "" 100

# Monitor with custom line count
monitor-lines count="200":
    @osascript tools/monitor-agent.scpt "" {{count}}

# Monitor a specific session
monitor-session pattern:
    @osascript tools/monitor-agent.scpt "{{pattern}}" 100

# Get session statistics (tokens, commands, etc.)
stats:
    @osascript tools/monitor-agent.scpt "" stats

# Get session statistics for specific window
stats-session pattern:
    @osascript tools/monitor-agent.scpt "{{pattern}}" stats

# Get full transcript
transcript:
    @osascript tools/monitor-agent.scpt "" full

# Save full transcript to file
save-transcript:
    @osascript tools/monitor-agent.scpt "" full > transcripts/session_$(date +%Y%m%d_%H%M%S).txt
    @echo "Transcript saved to transcripts/session_$(date +%Y%m%d_%H%M%S).txt"

# Monitor golars sessions specifically
golars:
    @osascript tools/monitor-agent.scpt "golars — ✳" 100

# Monitor golars with more context
golars-context:
    @osascript tools/monitor-agent.scpt "golars — ✳" 500

# Get golars session stats
golars-stats:
    @osascript tools/monitor-agent.scpt "golars — ✳" stats

# Extract conversation from current session
conversation:
    @osascript tools/monitor-agent.scpt "" conversation

# Extract and save conversation to context
save-conversation:
    @osascript tools/monitor-agent.scpt "" conversation > .claude/context_$(date +%Y%m%d_%H%M%S).md
    @echo "Conversation saved to .claude/context_$(date +%Y%m%d_%H%M%S).md"

# Extract conversation from specific session
conversation-session pattern:
    @osascript tools/monitor-agent.scpt "{{pattern}}" conversation

# Create a CLAUDE.md context file from current conversation
update-context:
    @mkdir -p .claude
    @echo "# Context from $(date +%Y-%m-%d)" > .claude/CLAUDE_context.md
    @echo "" >> .claude/CLAUDE_context.md
    @osascript tools/monitor-agent.scpt "" conversation >> .claude/CLAUDE_context.md
    @echo "Context updated in .claude/CLAUDE_context.md"

# Analyze user prompts
analyze-prompts:
    @osascript tools/monitor-agent.scpt "" prompts

# Show prompt statistics
prompt-stats:
    @osascript tools/monitor-agent.scpt "" prompt-stats

# Analyze prompts from specific session
analyze-prompts-session pattern:
    @osascript tools/monitor-agent.scpt "{{pattern}}" prompts

# Save prompt analysis
save-prompt-analysis:
    @mkdir -p .claude/prompt-analysis
    @osascript tools/monitor-agent.scpt "" prompts > .claude/prompt-analysis/analysis_$(date +%Y%m%d_%H%M%S).txt
    @echo "Prompt analysis saved to .claude/prompt-analysis/analysis_$(date +%Y%m%d_%H%M%S).txt"

# Watch agent activity (refresh every 5 seconds)
watch:
    @while true; do \
        clear; \
        osascript tools/monitor-agent.scpt "" 50; \
        sleep 5; \
    done

# Watch with stats
watch-stats:
    @while true; do \
        clear; \
        osascript tools/monitor-agent.scpt "" stats; \
        sleep 10; \
    done

# List all available Claude sessions
list-sessions:
    @osascript -e 'tell application "Terminal" to name of every window' | tr ',' '\n' | grep "✳" | sed 's/^ //'

# Show tool usage
help:
    @echo "Golars Monitoring Tools"
    @echo "======================"
    @echo ""
    @echo "Quick commands:"
    @echo "  just monitor          - View current agent activity"
    @echo "  just stats           - Show session statistics"
    @echo "  just transcript      - Get full session transcript"
    @echo "  just conversation    - Extract compact conversation"
    @echo "  just golars          - Monitor golars sessions"
    @echo "  just watch           - Live monitoring (updates every 5s)"
    @echo "  just list-sessions   - Show all Claude sessions"
    @echo ""
    @echo "Advanced usage:"
    @echo "  just monitor-lines 500                - Show last 500 lines"
    @echo "  just monitor-session 'Next Steps'     - Monitor specific window"
    @echo "  just save-transcript                  - Save transcript to file"
    @echo "  just save-conversation                - Save conversation to context"
    @echo "  just update-context                   - Update CLAUDE.md context"
    @echo ""
    @echo "Conversation commands:"
    @echo "  just conversation                     - View compact conversation"
    @echo "  just conversation-session 'pattern'   - Extract from specific session"
    @echo "  just save-conversation                - Save to timestamped file"
    @echo "  just update-context                   - Create/update context file"
    @echo ""
    @echo "Prompt analysis:"
    @echo "  just analyze-prompts                  - Analyze your prompts"
    @echo "  just prompt-stats                     - Show prompting statistics"
    @echo "  just save-prompt-analysis             - Save analysis to file"
    @echo ""
    @echo "Run 'just help' to see this message again"