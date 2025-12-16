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
    import { onMount } from 'svelte';
    import { goto } from '$app/navigation';
    import { Logout, UserAvatarFilledAlt } from 'carbon-icons-svelte';
    import { user, healthEnabled } from '$lib/stores'; 

    const AUTH_ENABLED = (env.PUBLIC_AUTH_ENABLED || 'true').toLowerCase() === 'true';

    let currentUser;
    user.subscribe(value => {
        currentUser = value;
    });

    let isHeaderActionOpen = false;
    let extra_links = [];
    let extra_header_links = [];
    let company = 'Apache Iceberg';
    let platform = 'Lakevision';

    // --- Configuration for Session Management ---
    const DEFAULT_AUTH_TOKEN_URL = '/api/auth/token';
    const AUTH_TOKEN_EXCHANGE_URL = env.PUBLIC_AUTH_TOKEN_EXCHANGE_URL || DEFAULT_AUTH_TOKEN_URL;
    const SESSION_CHECK_URL = env.PUBLIC_SESSION_CHECK_URL || '/api/auth/session'; 
    // --------------------------------------------------

    async function checkExistingSession() {
        if (!AUTH_ENABLED) return;
        console.log("Checking for existing session...");
        try {
            // Browser sends HttpOnly session cookie automatically
            const response = await fetch(SESSION_CHECK_URL);

            if (response.ok) {
                const data = await response.json();
                console.log("Session found, setting user:", data);
                user.set(data);
                showLogin = false;
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
        if (env.PUBLIC_HEALTH_ENABLED == 'true') {
            healthEnabled.set(true);
        } else {
            healthEnabled.set(false);
        }
        if (env.PUBLIC_EXTRA_LINKS) {
            try {
                extra_links = JSON.parse(env.PUBLIC_EXTRA_LINKS);
            } catch (e) {
                console.error("Failed to parse PUBLIC_EXTRA_LINKS. Ensure it is valid JSON.", e);
            }
        }
        if (env.PUBLIC_EXTRA_HEADER_LINKS) {
            try {
                extra_header_links = JSON.parse(env.PUBLIC_EXTRA_HEADER_LINKS);
            } catch (e) {
                console.error("Failed to parse PUBLIC_EXTRA_HEADER_LINKS. Ensure it is valid JSON.", e);
            }
        }
        if (env.PUBLIC_COMPANY_NAME) company = env.PUBLIC_COMPANY_NAME;
        if (env.PUBLIC_PLATFORM_NAME) platform = env.PUBLIC_PLATFORM_NAME;

        // Skip auth flow entirely when disabled; set a dev user so WS can connect.
        if (!AUTH_ENABLED) {
            user.set({"email":"dev-user@test.com"});
            showLogin = false;
            return;
        }

        const params = new URLSearchParams(window.location.search);
        const code = params.get('code');
        const state = params.get('state');

        if (code) {
            // 1. If code exists, exchange it (Redirect from IdP)
            exchangeCodeForToken(code, state);
        } else if (currentUser == null) {
            // 2. If no code and no user, check for existing session cookie (Refresh)
            const sessionFound = await checkExistingSession();
            if (!sessionFound) {
                // 3. If no session and no code, force login
                console.error('No authorization code found! Redirecting to login.');
                login();
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
            user.set(data);

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
        if (!browser || !AUTH_ENABLED) return;
        const params = new URLSearchParams(window.location.search);
        const state = encodeURIComponent(params.toString());
        window.location.href = `${openidProviderUrl}?client_id=${clientId}&redirect_uri=${redirectUri}&response_type=code&scope=openid%20profile%20email&state=${state}`;
        showLogin = false;
    }

    async function logout() {
        if (!AUTH_ENABLED) {
            user.set(null); 
            return;
        }
        await fetch('/api/logout', { method: 'POST' });
        user.set(null); 
        // Reload to clear client state and re-trigger session check (which will fail and redirect to login)
        if (browser) window.location.reload();
    }

    let showLogin = false;

    function handleLogout(event) {
        logout();
    }
</script>

<Header company="{company}" platformName="{platform}">
    <HeaderNavItem href="/" text="Home" />
    <HeaderNavItem href="/lh-health" text="Lakehouse Health" />
    
    {#if extra_header_links.length > 0}
        {#each extra_header_links as link}
            <HeaderNavItem 
                text="{link.text}" 
                href="{link.href}" 
                target="{link.target || '_blank'}" 
            />
        {/each}
    {/if}

    <HeaderUtilities>
        <HeaderActionLink href="https://github.com/IBM/lakevision" target="_blank">
            <LogoGithub slot="icon" size="{20}" />
        </HeaderActionLink>
        {#if AUTH_ENABLED}
            <HeaderGlobalAction iconDescription="{currentUser?.email || currentUser?.id || 'User'}" icon="{UserAvatarFilledAlt}" />
            <HeaderGlobalAction
                iconDescription="Logout"
                icon="{Logout}"
                on:click="{(event) => handleLogout(event)}"
            />
        {/if}
        {#if extra_links.length > 0}
            <HeaderAction bind:isOpen="{isHeaderActionOpen}">
                <HeaderPanelLinks>
                    {#each extra_links as link}
                        <HeaderPanelLink
                            text="{link.text}"
                            href="{link.href}"
                            target="_blank"
                        ></HeaderPanelLink>
                    {/each}
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

<slot></slot>