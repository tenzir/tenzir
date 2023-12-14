import React from "react";
import clsx from "clsx";
import { ThemeClassNames } from "@docusaurus/theme-common";
import { isActiveSidebarItem } from "@docusaurus/theme-common/internal";
import Link from "@docusaurus/Link";
import isInternalUrl from "@docusaurus/isInternalUrl";
import IconExternalLink from "@theme/Icon/ExternalLink";
import styles from "./styles.module.css";
import SVGSource from "./IconSource.svg";
import SVGTransformation from "./IconTransformation.svg";
import SVGSink from "./IconSink.svg";

export default function DocSidebarItemLink({
  item,
  onItemClick,
  activePath,
  level,
  index,
  ...props
}) {
  const { href, label, className, autoAddBaseUrl, customProps } = item;
  const isActive = isActiveSidebarItem(item, activePath);
  const isInternalLink = isInternalUrl(href);

  return (
    <li
      className={clsx(
        ThemeClassNames.docs.docSidebarItemLink,
        ThemeClassNames.docs.docSidebarItemLinkLevel(level),
        "menu__list-item",
        className
      )}
      key={label}
    >
      <Link
        className={clsx(
          "menu__link",
          !isInternalLink && styles.menuExternalLink,
          {
            "menu__link--active": isActive,
          }
        )}
        autoAddBaseUrl={autoAddBaseUrl}
        aria-current={isActive ? "page" : undefined}
        to={href}
        {...(isInternalLink && {
          onClick: onItemClick ? () => onItemClick(item) : undefined,
        })}
        {...props}
      >
        <div
          style={{
            width: "100%",
            display: "flex",
          }}
        >
          {label}
          <span
            style={{
              flexGrow: 1
            }}
          >
          </span>
          <span
            style={{
              visibility: customProps?.operator?.source ? "visible": "hidden"
            }}
          >
            <IconSource />
          </span>
          <span
            style={{
              visibility: customProps?.operator?.transformation ? "visible": "hidden"
            }}
          >
            <IconTransformation />
          </span>
          <span
            style={{
              visibility: customProps?.operator?.sink ? "visible": "hidden"
            }}
          >
            <IconSink />
          </span>
        </div>
        {!isInternalLink && <IconExternalLink />}
      </Link>
    </li>
  );
}

const IconContainer = ({ children }) => (
  <div
    style={{
      height: 20,
      marginRight: -5,
    }}
  >
    {children}
  </div>
);

const withIconContainer = (Icon) => () =>
  (
    <IconContainer>
      <Icon />
    </IconContainer>
  );

const IconSource = withIconContainer(SVGSource);
const IconTransformation = withIconContainer(SVGTransformation);
const IconSink = withIconContainer(SVGSink);
