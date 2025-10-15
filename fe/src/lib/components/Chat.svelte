<script>
    import { onMount, afterUpdate } from 'svelte';
    import { TextArea, Button, Loading } from 'carbon-components-svelte';
    import Send from 'carbon-icons-svelte/lib/Send.svelte';
    import TrashCan from 'carbon-icons-svelte/lib/TrashCan.svelte';
    import { env } from '$env/dynamic/public';
    import { marked } from 'marked';
    import DOMPurify from 'dompurify';
    import { BarChartSimple, LineChart, LollipopChart, AreaChart } from '@carbon/charts-svelte'

    /**
	 * @type {any}
	 */    
    let ChartComponents = {};
    
    onMount(async () => {        
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
        ChartComponents["BarChart"] = BarChartSimple;
        ChartComponents = {
                "BarChart": BarChartSimple,
                "LineChart": LineChart,
                "LollipopChart": LollipopChart,
                "AreaChart": AreaChart
        };
        setInputFocus();
    });

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
        console.log('Input ref set:', textInputElement); // Debug
        return {
            destroy() {
                textInputElement = null;
            }
        };
    }

    $: formattedMessages = messages.map(raw => {
        try {
            const parsed = JSON.parse(raw);
            console.log('Parsed message:', parsed);
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
                        charts: formatted.charts,
                        timestamp: formatted.timestamp
                    };
                    displayMessages = [...displayMessages];
                } else {
                    displayMessages = [...displayMessages, { ...formatted, id: msg.timestamp }];
                }
            }
        });
        console.log('Updated displayMessages:', displayMessages.map(m => ({ id: m.id, charts: m.charts })));
        if (messagesContainer) {
            messagesContainer.scrollBottom= messagesContainer.scrollHeight;
        }
    }

    onMount(() => {
        
    });

    afterUpdate(() => {
        if (!isSending) {
            setInputFocus();
        }
    });

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
    // Decode HTML entities (e.g., &quot; -> ")
    function decodeHtml(html) {
        const txt = document.createElement('textarea');
        txt.innerHTML = html;
        const decoded = txt.value;
        console.log('decodeHtml input:', html);
        console.log('decodeHtml output:', decoded);
        return decoded;
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
                role: user, //message.role,
                content: formatContent(message.content, message.role),
                timestamp: message.timestamp,
                charts: [],
                class: 'user'
            };
        } else if (message.role === 'assistant' || message.role === 'model') {
            if (!currentAssistantMessageId) {
                currentAssistantMessageId = messageId;
                lastAssistantContent = message.content || '';
                const { html, charts } = formatContent(message.content);
                console.log("Got charts "+charts);
                return {
                    role: message.role,
                    content: html,
                    charts,
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
                    const { html, charts } = formatContent(lastAssistantContent);
                    console.log("Got charts2 "+charts.length);
                    return {
                        role: message.role,
                        content: html,
                        charts,
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
                content: formatContent(message.content, "system"),
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

    function formatContent(text, role="") {
        try {
            console.log('Raw Markdown:', text);
            if (typeof window === 'undefined') {
                console.log('Skipping chart rendering on server-side');
                return { html: (text || '').replace(/\n/g, '<br>'), charts: [] };
            }
            let html = marked.parse(text || '', { gfm: true, breaks: true, tables: true });
            let charts = [];
            // Detect and replace chart code blocks
            html = html.replace(/<pre><code class="language-chart">([\s\S]*?)<\/code><\/pre>/g, (match, jsonStr) => {

            if(role!="user" ){ //&& !text.includes("bar chart")){
                
                    try {
                        const decodedJson = decodeHtml(jsonStr.trim());
                        console.log('Decoded JSON:', decodedJson);
                        const chartConfig = JSON.parse(decodedJson);
                        console.log('Parsed chart config:', chartConfig);

                        // Validate required fields
                        if (!chartConfig.type) {
                            console.warn('Chart missing type:', chartConfig);
                            return `<pre><code>${jsonStr}</code></pre>`;
                        }
                        if (!chartConfig.data) {
                            console.warn('Chart missing data or labels:', chartConfig);
                            return `<pre><code>${jsonStr}</code></pre>`;
                        }
                        if (!chartConfig.options || !chartConfig.options.axes) {
                            console.warn('Chart missing axes configuration:', chartConfig);
                            return `<pre><code>${jsonStr}</code></pre>`;
                        }

                        const chartType = chartConfig.type.charAt(0).toUpperCase() + chartConfig.type.slice(1) + 'Chart';
                        console.log('Normalized chart type:', chartType);

                        if (ChartComponents[chartType]) {
                            const chartId = `chart-${Math.random().toString(36).substr(2, 9)}`;
                            charts.push({
                                id: chartId,
                                component: ChartComponents[chartType],
                                data: chartConfig.data,
                                options: chartConfig.options
                            });
                            console.log('Chart added to charts:', { id: chartId, type: chartType });
                            return `<div class="carbon-chart" id="${chartId}"></div>`;                            
                        } else {
                            console.warn(`Chart type ${chartType} not supported or not loaded`);
                            return `<pre><code>${jsonStr}</code></pre>`;
                        }
                    
                    } catch (e) {
                        console.log('Chart parsing error');
                        return `<pre><code>${jsonStr}</code></pre>`;                      
                    }
                }
                    });
                console.log('Rendered HTML:', html);            
                console.log('Charts extracted:', charts);
                if ((role== "user" || role== "system" ) && charts.length==0) return DOMPurify.sanitize(html);
                return { html: DOMPurify.sanitize(html), charts };

            } catch (e) {
                console.error('Markdown parsing error:', e);
                return { html: (text || '').replace(/\n/g, '<br>'), charts: [] };
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
    <div class="details">
        <p>You can find out information about you lakehouse like:</p>
        <ul>
            <li>List namespaces</li>
            <li>List tables in namespace X</li>
            <li>Describe table X.Y</li>
        </ul>
    </div>
    <div class="messages" bind:this={messagesContainer}>
        {#each displayMessages as msg (msg.id)}
            <div class="message {msg.class}">
                <div class="role">{msg.role}</div>
                <div class="timestamp">{formatTime(msg.timestamp)}</div>
                <div class="markdown" class:markdown--user={msg.class === 'user'}>
                    {@html msg.content}
                </div>
                
                {#if msg.charts}                
                {#each msg.charts as chart (chart.id)}                
                    <div class="carbon-chart" id={chart.id}>                        
                        {#if chart.component}                                
                            <svelte:component this={chart.component} data={chart.data} options={chart.options} />
                        {:else}
                            <p>Chart component not available</p>
                        {/if}
                    </div>
                {/each}
                {/if}
                
            </div>
        {/each}
    </div>
    {#if isSending}        
        <Loading withOverlay={false} small />        
    {/if}
    <div class="input-area">
        <TextArea
            rows={3}
            bind:value={message}
            placeholder="Type your message..."
            on:keydown={handleKeyDown}
            disabled={isSending}
        >
            <input use:inputRef />
        </TextArea>
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
    .carbon-chart {
        margin: 10px 0;
        width: 100%;
        height: 400px; 
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
        padding-right: 10px;
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

    .chat-container ul {
    list-style-type: none; /* Remove default bullets */
    padding-left: 0; /* Remove default padding */
    }

    .chat-container ul li {
    background-image: url("checkmark.svg"); /* Custom bullet image */
    background-repeat: no-repeat;
    background-position: 0 50%; /* Center image vertically */
    background-size: 1.2em; /* Adjust image size */
    padding-left: 1.5em; /* Space for the custom bullet */
    margin-bottom: 0.5em;
    color: #333;
    }
</style>