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
	import { healthEnabled } from '$lib/stores';

	let user;
	let isHeaderActionOpen = false;
	let AUTH_ENABLED = false;
	let extra_links = [];
	let extra_header_links = [];
	let company = 'Apache Iceberg';
	let platform = 'Lakevision';

	onMount(() => {
		if (env.PUBLIC_AUTH_ENABLED == 'true') {
			AUTH_ENABLED = true;
		}

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
		if (AUTH_ENABLED && user == null) {
			const params = new URLSearchParams(window.location.search);
			const code = params.get('code');
			const state = params.get('state');
			if (code) {
				exchangeCodeForToken(code, state);
			} else {
				console.error('No authorization code found!');
				login();
			}
		}
	});

	async function exchangeCodeForToken(code, state) {
		const response = await fetch('/api/auth/token', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ code }),
			credentials: 'include'
		});

		if (response.ok) {
			const data = await response.json();
			console.log('Token received:', data);
			user = data;

			// Decode the state parameter and redirect, preserving the original URL query
			const decodedState = decodeURIComponent(state || '');
			if (decodedState) {
				goto('/?' + decodedState);
			} else {
				// stay on the current path; don't wipe params
				goto($page.url.pathname, { replaceState: true });
			}
		} else {
			console.error('Error exchanging token');
		}
	}

	const clientId = env.PUBLIC_OPENID_CLIENT_ID;
	const openidProviderUrl = env.PUBLIC_OPENID_PROVIDER_URL + '/authorize';
	const redirectUri = encodeURIComponent(env.PUBLIC_REDIRECT_URI);

	function login() {
		const params = new URLSearchParams(window.location.search);
		const state = encodeURIComponent(params.toString());
		window.location.href = `${openidProviderUrl}?client_id=${clientId}&redirect_uri=${redirectUri}&response_type=code&scope=openid%20profile%20email&state=${state}`;
		showLogin = false;
	}

	async function logout() {
		await fetch('/api/logout');
		user = '';
		return;
	}

	let showLogin = false;
	let loginPosition = { top: 0, left: 0 };

	function handleLogout(event) {
		logout();
		showLogin = true;
		const rect = event.target.getBoundingClientRect();
		loginPosition = {
			top: rect.top + rect.height + window.scrollY,
			left: rect.left + window.scrollX
		};
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
			<HeaderGlobalAction iconDescription="{user}" icon="{UserAvatarFilledAlt}" />
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