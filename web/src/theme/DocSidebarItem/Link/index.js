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
import SVGLoader from "./IconLoader.svg";
import SVGSaver from "./IconSaver.svg";
import SVGParser from "./IconParser.svg";
import SVGPrinter from "./IconPrinter.svg";

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
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          {label}
          <div
            style={{
              display: "flex",
            }}
          >
            {customProps?.operator?.source && <IconSource />}
            {customProps?.operator?.transformation && <IconTransformation />}
            {customProps?.operator?.sink && <IconSink />}
            {customProps?.connector?.loader && <IconLoader />}
            {customProps?.connector?.saver && <IconSaver />}
            {customProps?.format?.parser && <IconParser />}
            {customProps?.format?.printer && <IconPrinter />}
          </div>
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
      //marginRight: -10,
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
const IconLoader = withIconContainer(SVGLoader);
const IconSaver = withIconContainer(SVGSaver);
const IconParser = withIconContainer(SVGParser);
const IconPrinter = withIconContainer(SVGPrinter);
