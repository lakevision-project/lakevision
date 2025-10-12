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

	export let data;
	let user;
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


	onMount(() => {
		if (env.PUBLIC_AUTH_ENABLED == 'true') {
			AUTH_ENABLED = true;
		}
		if (env.PUBLIC_CHAT_ENABLED == 'true') {
			if (env.PUBLIC_CHAT_NAME) {
				CHAT_NAME = env.PUBLIC_CHAT_NAME;
			}
			CHAT_ENABLED = true;
		}
		if (env.PUBLIC_EXTRA_LINK) {
			extra_link = env.PUBLIC_EXTRA_LINK;
			extra_link_text = env.PUBLIC_EXTRA_LINK_TEXT;
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
			goto('/?' + decodedState);
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
	<HeaderNavItem href="/" text="Tables" />
	<HeaderNavItem href="/lh-health" text="Lakehouse Health" />
	
	{#if CHAT_ENABLED}
		<HeaderNavItem text="{CHAT_NAME}" href="?openChat=true" />
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
	passiveModal
	bind:open={isChatOpen}
	modalHeading="Chat with your Datasets"
	on:open={handleChatOpen}
	on:close={handleChatClose}
	on:click:overlay={handleChatClose}
>
	{#if user}
		<Chat {user} />
	{/if}
</Modal>

<slot></slot>