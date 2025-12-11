<script>
    import { env } from '$env/dynamic/public';
    import { browser } from '$app/environment';
    import 'carbon-components-svelte/css/all.css';
    import '../app.css';
    import { page } from '$app/stores';
    import {
        Header,
        HeaderUtilities,
        HeaderNavItem,
        HeaderActionLink,
        HeaderPanelLinks,
        HeaderGlobalAction,
        HeaderPanelLink,
        HeaderAction,
        Modal
    } from 'carbon-components-svelte';

    import LogoGithub from 'carbon-icons-svelte/lib/LogoGithub.svelte';
    import { onMount, tick } from 'svelte';
    import { goto, afterNavigate } from '$app/navigation';
    import { Logout, UserAvatarFilledAlt } from 'carbon-icons-svelte';
    import Chat from '../lib/components/Chat.svelte';
    
    import { healthEnabled, user } from '$lib/stores'; 

    let currentUser;
    user.subscribe(value => {
        currentUser = value;
    });
    
    let isHeaderActionOpen = false;
    let AUTH_ENABLED = false;
    let CHAT_ENABLED = false;
    let CHAT_NAME = 'Chat';
    let extra_link;
    let extra_link_text;
    let company = 'Apache Iceberg';
    let platform = 'Lakevision';

    let isChatOpen = false;
    let ignoreUrlSync = false; // guard to prevent re-opening during our own updates

    // --- NEW: Configuration Endpoints ---
    const DEFAULT_AUTH_TOKEN_URL = '/api/auth/token';
    const AUTH_TOKEN_EXCHANGE_URL = env.PUBLIC_AUTH_TOKEN_EXCHANGE_URL || DEFAULT_AUTH_TOKEN_URL;
    // Endpoint must be provided by the backend to check if the session is valid
    const SESSION_CHECK_URL = env.PUBLIC_SESSION_CHECK_URL || '/api/auth/session'; 
    // --- END NEW CONFIG ---

    // initialize from current URL once on mount
    onMount(() => {
        isChatOpen = $page.url.searchParams.get('openChat') === 'true';
    });

    // only sync when navigation finishes (URL actually changed)
    afterNavigate(() => {
        if (ignoreUrlSync) return; // skip if change initiated by setChatOpen
        isChatOpen = $page.url.searchParams.get('openChat') === 'true';
    });

    async function setChatOpen(open) {
        if (!browser) return;
        // immediately reflect in UI
        isChatOpen = open;

        // update URL without adding history entries
        const sp = new URLSearchParams($page.url.searchParams);
        open ? sp.set('openChat', 'true') : sp.delete('openChat');
        const qs = sp.toString();

        ignoreUrlSync = true;
        await goto(qs ? `?${qs}` : $page.url.pathname, { replaceState: true, noScroll: true });
        await tick();               // let Svelte flush
        ignoreUrlSync = false;      // re-enable URL → state syncing
    }

    const handleChatClose = () => setChatOpen(false);
    const handleChatOpen  = () => setChatOpen(true);

    async function checkExistingSession() {
        console.log("Checking for existing session...");
        try {
            // Browser sends HttpOnly cookie automatically
            const response = await fetch(SESSION_CHECK_URL);

            if (response.ok) {
                const data = await response.json();
                console.log("Session found, setting user:", data);
                user.set(data.email); // Set full user object in the store
                return true;
            } else {
                console.log("No existing session found.");
                return false;
            }
        } catch (error) {
            console.error("Error checking session:", error);
            return false;
        }
    }

    onMount(async () => {
        if (env.PUBLIC_AUTH_ENABLED == 'true') {
            AUTH_ENABLED = true;
        }
        if (env.PUBLIC_CHAT_ENABLED == 'true') {
            if (env.PUBLIC_CHAT_NAME) {
                CHAT_NAME = env.PUBLIC_CHAT_NAME;
            }
            CHAT_ENABLED = true;
        }

        if (env.PUBLIC_HEALTH_ENABLED == 'true') {
            healthEnabled.set(true);
        } else {
            healthEnabled.set(false);
        }
        if (env.PUBLIC_EXTRA_LINK) {
            extra_link = env.PUBLIC_EXTRA_LINK;
            extra_link_text = env.PUBLIC_EXTRA_LINK_TEXT;
        }
        if (env.PUBLIC_COMPANY_NAME) company = env.PUBLIC_COMPANY_NAME;
        if (env.PUBLIC_PLATFORM_NAME) platform = env.PUBLIC_PLATFORM_NAME;
        
        if (AUTH_ENABLED) {
            const params = new URLSearchParams(window.location.search);
            const code = params.get('code');
            const state = params.get('state');

            if (code) {
                exchangeCodeForToken(code, state);
            } else if (currentUser === null) {
                const sessionFound = await checkExistingSession();
                if (!sessionFound) {
                    console.error('No authorization code found! Redirecting to login.');
                    login();
                }
            }
        }
    });

    async function exchangeCodeForToken(code, state) {
        const response = await fetch(AUTH_TOKEN_EXCHANGE_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code }),
            credentials: 'include'
        });

        if (response.ok) {
            const data = await response.json();
            console.log('Token exchanged, user data received:', data);
            user.set(data.email); // Store the full user object

            // Decode the state parameter and redirect, preserving the original URL query
            const decodedState = decodeURIComponent(state || '');
            if (decodedState) {
                goto('/?' + decodedState, { replaceState: true });
            } else {
                // stay on the current path, cleaning up the code/state params
                goto($page.url.pathname, { replaceState: true });
            }
        } else {
            console.error('Error exchanging token');
            login(); // Redirect on failure
        }
    }

    const clientId = env.PUBLIC_OPENID_CLIENT_ID;
    const openidProviderUrl = env.PUBLIC_OPENID_PROVIDER_URL + '/authorize';
    const redirectUri = encodeURIComponent(env.PUBLIC_REDIRECT_URI);

    function login() {
        if (!browser) return;
        const params = new URLSearchParams(window.location.search);
        const state = encodeURIComponent(params.toString());
        window.location.href = `${openidProviderUrl}?client_id=${clientId}&redirect_uri=${redirectUri}&response_type=code&scope=openid%20profile%20email&state=${state}`;
        showLogin = false;
    }

    async function logout() {
        await fetch('/api/logout', { method: 'POST' });
        user.set(null); // Clear store
        // Force reload to trigger session check, which will now fail and redirect to login.
        if (browser) window.location.reload();
    }

    let showLogin = false;
    let loginPosition = { top: 0, left: 0 };

    function handleLogout(event) {
        logout();
    }
</script>

<Header company="{company}" platformName="{platform}">
    <HeaderNavItem href="/" text="Home" />
    <HeaderNavItem href="/lh-health" text="Lakehouse Health" />
    
    {#if CHAT_ENABLED}
        <HeaderNavItem text="{CHAT_NAME}" href="?openChat=true" />
    {/if}


    <HeaderUtilities>
        <HeaderActionLink href="https://github.com/IBM/lakevision" target="_blank">
            <LogoGithub slot="icon" size="{20}" />
        </HeaderActionLink>
        {#if AUTH_ENABLED}
            <!-- Display user details from the store object -->
            <HeaderGlobalAction iconDescription="{currentUser || 'User'}" icon="{UserAvatarFilledAlt}" />
            <HeaderGlobalAction
                iconDescription="Logout"
                icon="{Logout}"
                on:click="{(event) => handleLogout(event)}"
            />
        {/if}
        {#if extra_link}
            <HeaderAction bind:isOpen="{isHeaderActionOpen}">
                <HeaderPanelLinks>
                    <HeaderPanelLink
                        text="{extra_link_text}"
                        href="{extra_link}"
                        target="_blank"
                    ></HeaderPanelLink>
                </HeaderPanelLinks>
            </HeaderAction>
        {/if}
    </HeaderUtilities>
</Header>

<Modal
    bind:open="{showLogin}"
    modalHeading="Login"
    primaryButtonText="Login"
    secondaryButtonText="Cancel"
    on:click:button--secondary="{() => (showLogin = false)}"
    on:submit="{login}"
/>

<Modal
    size='lg'
    passiveModal
    bind:open={isChatOpen}
    modalHeading="{CHAT_NAME}"
    on:open={handleChatOpen}
    on:close={handleChatClose}
    on:click:overlay={handleChatClose}
>
    {#if currentUser}
        <!-- Pass the full currentUser object to Chat -->
        <Chat user={currentUser} /> 
    {/if}
</Modal>

<slot></slot>