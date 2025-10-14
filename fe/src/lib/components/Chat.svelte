<script>
    import { onMount, afterUpdate } from 'svelte';
    import { TextInput, Button, Tag } from 'carbon-components-svelte';
    import Send from 'carbon-icons-svelte/lib/Send.svelte';
    import TrashCan from 'carbon-icons-svelte/lib/TrashCan.svelte';
    import IbmGranite from 'carbon-icons-svelte/lib/IbmGranite.svelte';
    import { env } from '$env/dynamic/public';
    import { marked } from 'marked';
    import DOMPurify from 'dompurify';

    export let user;
    let messages = [];
    let message = '';
    let status = 'Ready';
    let isSending = false;
    let messagesContainer;
    let textInputElement; // Ref to native <input> element
    let currentAssistantMessageId = null;
    let lastAssistantContent = '';

    let displayMessages = [];
    let processedMessages = new Map();

    // Action to get native <input> element
    function inputRef(node) {
        textInputElement = node;        
        return {
            destroy() {
                textInputElement = null;
            }
        };
    }

    $: formattedMessages = messages.map(raw => {
        try {
            const parsed = JSON.parse(raw);
            return parsed;
        } catch (e) {
            console.error('Failed to parse JSON:', e, raw);
            return { role: 'system', content: `Parse error: ${e.message}`, timestamp: new Date().toISOString() };
        }
    });

    $: {
        formattedMessages.forEach(msg => {
            const formatted = handleMessage(msg);
            if (formatted) {
                const existingIndex = displayMessages.findIndex(dm => dm.id === formatted.updateId || dm.id === msg.timestamp);
                if (formatted.updateId && existingIndex !== -1) {
                    displayMessages[existingIndex] = {
                        ...displayMessages[existingIndex],
                        content: formatted.content,
                        timestamp: formatted.timestamp
                    };
                    displayMessages = [...displayMessages];
                } else {
                    displayMessages = [...displayMessages, { ...formatted, id: msg.timestamp }];
                }
            }
        });
        if (messagesContainer) {
            messagesContainer.scrollBottom= messagesContainer.scrollHeight;
        }
    }

    onMount(() => {
        user = user.split('@')[0];
        if (typeof marked === 'undefined' || typeof DOMPurify === 'undefined') {
            console.error('marked or DOMPurify not loaded');
            status = 'Error: Markdown renderer or sanitizer not loaded. Tables may not display correctly.';
        }
        marked.setOptions({
            gfm: true,
            breaks: true,
            tables: true
        });
        setInputFocus();
    });

    afterUpdate(() => {
        if (!isSending) {
            setInputFocus();
        }
    });
    
    //not working
    function setInputFocus() {
        if (textInputElement) {
            console.log('Setting focus on textInputElement:', textInputElement); // Debug
            textInputElement.focus();
        } else {
            console.warn('textInputElement is not defined');
        }
    }

    async function sendMessage() {
        if (!message.trim() || isSending) return;

        isSending = true;
        status = 'Streaming response...';
        currentAssistantMessageId = null;
        lastAssistantContent = '';

        try {
            const response = await fetch(`${env.PUBLIC_CHAT_SERVER}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: new URLSearchParams({ prompt: message }),
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            if (!response.body) {
                throw new Error('ReadableStream not supported');
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                let lineEnd;
                while ((lineEnd = buffer.indexOf('\n')) !== -1) {
                    const line = buffer.slice(0, lineEnd).trim();
                    buffer = buffer.slice(lineEnd + 1);

                    if (line) {
                        console.log('Received line:', line);
                        messages = [...messages, line];
                    }
                }
            }
        } catch (err) {
            console.error('Fetch error:', err);
            let errorMsg = err.message;
            if (err.name === 'TypeError' && err.message.includes('fetch')) {
                errorMsg = 'Failed to fetch. Check: 1) Server running? 2) Same origin? 3) CORS enabled? 4) Network/firewall?';
            }
            messages = [...messages, JSON.stringify({
                role: 'system',
                content: `Error: ${errorMsg}`,
                timestamp: new Date().toISOString(),
            })];
        } finally {
            isSending = false;
            status = 'Ready';
            message = '';
            // Focus handled by afterUpdate
        }
    }

    function clearMessages() {
        messages = [];
        displayMessages = [];
        processedMessages.clear();
        currentAssistantMessageId = null;
        lastAssistantContent = '';
        // Focus handled by afterUpdate
    }

    function handleKeyDown(event) {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            sendMessage();
        }
    }

    function formatTime(timestamp) {
        const date = new Date(timestamp);
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }

    function handleMessage(message) {
        const messageId = message.timestamp;
        console.log('Handling message:', { content: message.content, lastAssistantContent, messageId });

        if (processedMessages.has(messageId)) {
            console.log('Skipping duplicate message:', messageId);
            return null;
        }
        processedMessages.set(messageId, message);

        if (message.role === 'user') {
            lastAssistantContent = '';
            currentAssistantMessageId = null;
            return {
              role: user, //message.role,         //todo: use message.role when user is not there (i.e. no auth)
                content: formatContent(message.content),
                timestamp: message.timestamp,
                class: 'user'
            };
        } else if (message.role === 'assistant' || message.role === 'model') {
            if (!currentAssistantMessageId) {
                currentAssistantMessageId = messageId;
                lastAssistantContent = message.content || '';
                return {
                    role: 'lh_assist', //message.role,    //externalize it
                    content: formatContent(message.content),
                    timestamp: message.timestamp,
                    class: 'assistant'
                };
            } else {
                const newText = message.content && lastAssistantContent
                    ? message.content.slice(lastAssistantContent.length)
                    : message.content || '';
                console.log('New text:', newText);
                if (newText) {
                    lastAssistantContent = message.content || '';
                    return {
                        role: message.role,
                        content: formatContent(lastAssistantContent),
                        timestamp: message.timestamp,
                        class: 'assistant',
                        updateId: currentAssistantMessageId
                    };
                }
                return null;
            }
        } else if (message.role === 'system') {
            return {
                role: message.role,
                content: formatContent(message.content),
                timestamp: message.timestamp,
                class: 'error'
            };
        } else {
            console.warn(`Unexpected message role: ${message.role}`);
            return {
                role: message.role,
                content: formatContent(message.content),
                timestamp: message.timestamp,
                class: 'system'
            };
        }
    }

    function formatContent(text) {
        try {
            console.log('Raw Markdown:', text);
            const html = marked.parse(text || '', { gfm: true, breaks: true, tables: true });
            console.log('Rendered HTML:', html);
            return DOMPurify.sanitize(html);
        } catch (err) {
            console.error('Markdown parsing error:', err);
            return (text || '').replace(/\n/g, '<br>');
        }
    }
</script>

<div class="header">
    <Button
        on:click={clearMessages}
        iconDescription="Clear Messages"
        icon={TrashCan}
        disabled={messages.length === 0}
    />
</div>
<div class="chat-container">
    <div class="status">{status}</div>
    <div class="messages" bind:this={messagesContainer}>
        {#each displayMessages as msg (msg.id)}
            <div class="message {msg.class}">
                <div class="role">{msg.role}</div>
                <div class="timestamp">{formatTime(msg.timestamp)}</div>
                <div class="markdown" class:markdown--user={msg.class === 'user'}>
                    {@html msg.content}
                </div>
            </div>
        {/each}
    </div>
    <div class="input-area">
        <TextInput
            bind:value={message}
            placeholder="Type your message..."
            on:keydown={handleKeyDown}
            disabled={isSending}
        >
            <input use:inputRef />
        </TextInput>
        <Button
            on:click={sendMessage}
            iconDescription="Send Message"
            icon={Send}
            size="sm"
            disabled={isSending || !message.trim()}
        />
    </div>
</div>

<style>
    .chat-container {
        width: 120vw;
        max-width: 1350px;
        margin: 20px auto;
        border: 1px solid #ccc;
        padding: 5px;
        display: flex;
        flex-direction: column;
    }

    .status {
        margin-bottom: 10px;
        color: orange;
    }

    .messages {
        flex-grow: 1;
        min-height: 200px;
        overflow-y: auto;
        margin-bottom: 10px;
    }

    .input-area {
        display: flex;
    }

    .header {
        display: flex;
        justify-content: flex-end;
        margin-bottom: 10px;
    }

    .message {
        display: flex;
        flex-direction: column;
        margin-bottom: 5px;
    }

    .message.user {
        background-color: #e2e5e6;
        align-items: flex-end;
    }

    .message.assistant {
        color: green;
    }

    .message.error {
        color: red;
        font-style: italic;
        align-items: center;
    }

    .message.system {
        color: gray;
        font-style: italic;
        align-items: center;
    }

    .role {
        font-weight: bold;
        font-size: 0.8em;
    }

    .timestamp {
        font-size: 0.7em;
        color: #666;
    }

    .markdown {
        white-space: pre-wrap;
        overflow-wrap: break-word;
    }

    .markdown--user {
        color: blue;
    }

    .markdown table {
        border-collapse: collapse;
        margin: 10px 0;
        width: 100%;
        font-size: 0.9em;
    }

    .markdown th,
    .markdown td {
        border: 1px solid #ccc;
        padding: 8px;
        text-align: left;
        vertical-align: top;
    }

    .markdown th {
        background-color: #f0f0f0;
        font-weight: bold;
    }

    .markdown ul {
        margin: 10px 0;
        padding-left: 20px;
    }

    .markdown li {
        margin-bottom: 5px;
    }

    .markdown strong {
        font-weight: bold;
    }

    .input-area :global(.bx--text-input:focus) {
        outline: 2px solid #0f62fe;
        outline-offset: 2px;
    }
</style>