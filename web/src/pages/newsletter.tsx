import React, { useEffect } from "react";
import Layout from "@theme/Layout";

export default function Newsletter() {
  useEffect(() => {
    const script = document.createElement("script");
    script.async = true;
    script.innerHTML = `(function(w,d,e,u,f,l,n){w[f]=w[f]||function(){(w[f].q=w[f].q||[])
      .push(arguments);},l=d.createElement(e),l.async=1,l.src=u,
      n=d.getElementsByTagName(e)[0],n.parentNode.insertBefore(l,n);})
      (window,document,'script','https://assets.mailerlite.com/js/universal.js','ml');
      ml('account', '918320');`;
    document.body.appendChild(script);

    return () => {
      document.body.removeChild(script);
    };
  }, []);

  return (
    <Layout title="Newsletter" description="Newsletter">
      <div
        style={{
          padding: "40px",
        }}
      >
        <div className="ml-embedded" data-form="YHfC4B"></div>
      </div>
    </Layout>
  );
}
