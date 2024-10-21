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
import Experimental from "./Experimental.svg";

const IconContainer = ({ children, title, marginLeft }) => (
  <div
    style={{
      display: 'flex',
      alignItems: 'center',
      height: 20,
      marginLeft: marginLeft,
      marginRight: -5,
    }}
    title={title}
  >
    {children}
  </div>
);

const withIconContainer = (Icon, title, marginLeft) => () =>
  (
    <IconContainer title={title} marginLeft={marginLeft}>
      <Icon style={{ height: '100%', width: 'auto' }} />
    </IconContainer>
  );

const IconSourceOn = withIconContainer(SourceOn, "Source", 0);
const IconSourceOff = withIconContainer(SourceOff, "", 0);
const IconTransformationOn = withIconContainer(TransformationOn, "Transformation", 0);
const IconTransformationOff = withIconContainer(TransformationOff, "", 0);
const IconSinkOn = withIconContainer(SinkOn, "Sink", 0);
const IconSinkOff = withIconContainer(SinkOff, "", 0);
const IconParserOn = withIconContainer(SourceOn, "Parser", 0);
const IconParserOff = withIconContainer(SourceOff, "", 0);
const IconPrinterOn = withIconContainer(SinkOn, "Printer", 0);
const IconPrinterOff = withIconContainer(SinkOff, "", 0);
const IconLoaderOn = withIconContainer(SourceOn, "Loader", 0);
const IconLoaderOff = withIconContainer(SourceOff, "", 0);
const IconSaverOn = withIconContainer(SinkOn, "Saver", 0);
const IconSaverOff = withIconContainer(SinkOff, "", 0);
const IconExperimental = withIconContainer(Experimental, "Experimental", 5);

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
          {customProps?.experimental && <IconExperimental />}
          <span
            style={{
              flexGrow: 1
            }}
          >
          </span>
          <SidebarIcons customProps={customProps} />
        </div>
        {!isInternalLink && <IconExternalLink />}
      </Link>
    </li>
  );
}

const SidebarIcons = ({ customProps }) => {
  let content = null;

  if (customProps?.operator) {
    content = (
      <>
        {customProps.operator.source ? <IconSourceOn /> : <IconSourceOff />}
        {customProps.operator.transformation ? <IconTransformationOn /> : <IconTransformationOff />}
        {customProps.operator.sink ? <IconSinkOn /> : <IconSinkOff />}
      </>
    );
  } else if (customProps?.connector) {
    content = (
      <>
        {customProps.connector.loader ? <IconLoaderOn /> : <IconLoaderOff />}
        {customProps.connector.saver ? <IconSaverOn /> : <IconSaverOff />}
      </>
    );
  } else if (customProps?.format) {
    content = (
      <>
        {customProps.format.parser ? <IconParserOn /> : <IconParserOff />}
        {customProps.format.printer ? <IconPrinterOn /> : <IconPrinterOff />}
      </>
    );
  }

  return (
    <>
      {content}
    </>
  );
};
