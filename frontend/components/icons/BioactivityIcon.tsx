const BioactivityIcon = (props: any) => {
  const { height = "1em", width = "1em", color = "black", ...rest } = props;

  return (
    <svg
      fill="none"
      stroke={color}
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth={3}
      height={height}
      width={width}
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 48 48"
      xmlSpace="preserve"
      {...rest}
    >
      <path d="M4 24h6l4-12 8 24 4-16 4 8h14" />
    </svg>
  );
};

BioactivityIcon.displayName = "BioactivityIcon";

export default BioactivityIcon;
