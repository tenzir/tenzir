import React from "react";
import clsx from "clsx";
import { ThemeClassNames } from "@docusaurus/theme-common";
import { isActiveSidebarItem } from "@docusaurus/theme-common/internal";
import Link from "@docusaurus/Link";
import isInternalUrl from "@docusaurus/isInternalUrl";
import IconExternalLink from "@theme/Icon/ExternalLink";
import styles from "./styles.module.css";
import SourceOn from "./SourceOn.svg";
import SourceOff from "./SourceOff.svg";
import TransformationOn from "./TransformationOn.svg";
import TransformationOff from "./TransformationOff.svg";
import SinkOn from "./SinkOn.svg";
import SinkOff from "./SinkOff.svg";

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
          <OperatorSidebarIcons customProps={customProps} />
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
      <Icon style={{ height: '100%', width: 'auto' }} />
    </IconContainer>
  );

const IconSourceOn = withIconContainer(SourceOn);
const IconSourceOff = withIconContainer(SourceOff);
const IconTransformationOn = withIconContainer(TransformationOn);
const IconTransformationOff = withIconContainer(TransformationOff);
const IconSinkOn = withIconContainer(SinkOn);
const IconSinkOff = withIconContainer(SinkOff);

const OperatorSidebarIcons = ({ customProps }) => {
  let content = null;

  if (customProps?.operator) {
    content = (
      <>
        {customProps.operator.source ? <IconSourceOn /> : <IconSourceOff />}
        {customProps.operator.transformation ? <IconTransformationOn /> : <IconTransformationOff />}
        {customProps.operator.sink ? <IconSinkOn /> : <IconSinkOff />}
      </>
    );
  }

  return (
    <>
      {content}
    </>
  );
};
