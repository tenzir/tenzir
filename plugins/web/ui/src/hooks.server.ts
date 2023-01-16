import { SvelteKitAuth } from '@auth/sveltekit';
import GitHub, { type GithubProfile } from '@auth/core/providers/github';
import { GITHUB_ID, GITHUB_SECRET } from '$env/static/private';

import { redirect, type Handle } from '@sveltejs/kit';
import { sequence } from '@sveltejs/kit/hooks';

async function authorization({ event, resolve }) {
  // Protect all pages except the login page
  if (!event.url.pathname.startsWith('/auth')) {
    const session = await event.locals.getSession();
    if (!session) {
      throw redirect(303, '/auth');
    }
  }

  // If the request is still here, just proceed as normally
  const result = await resolve(event, {
    transformPageChunk: ({ html }) => html
  });
  return result;
}

// First handle authentication, then authorization
// Each function acts as a middleware, receiving the request handle
// And returning a handle which gets passed to the next function
export const handle: Handle = sequence(
  SvelteKitAuth({
    // Using callbacks to get the user profile, which is used to filter
    // the users that can access the app (only the users belonging to
    // the organization tenzir are allowed)
    // https://next-auth.js.org/configuration/callbacks

    callbacks: {
      async signIn({ profile }) {
        const organizationsNames = await fetch((profile as GithubProfile)?.organizations_url)
          .then((res) => res.json())
          .then((orgs) => orgs.map((org) => org.login));

        const isAllowedToSignIn = organizationsNames.includes('tenzir');
        if (isAllowedToSignIn) {
          return true;
        } else {
          // Return false to display a default error message
          return false;
          // Or you can return a URL to redirect to:
          // return '/unauthorized'
        }
      }
    },
    // providers: [GitHub({ clientId: GITHUB_ID, clientSecret: GITHUB_SECRET })]
    providers: [GitHub({ clientId: GITHUB_ID, clientSecret: GITHUB_SECRET })]
  }),
  authorization
);
