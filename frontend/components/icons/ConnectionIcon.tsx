const ConnectionIcon = (props: any) => {
  const { height = "1em", width = "1em", color = "black", ...rest } = props;

  return (
    <svg
      fill={color}
      height={height}
      width={width}
      viewBox="0 0 16 16"
      xmlns="http://www.w3.org/2000/svg"
      xmlSpace="preserve"
      {...rest}
    >
      <path d="M12 10c-.601 0-1.134.27-1.5.69L5.954 8.419a2 2 0 000-.836l4.544-2.273c.367.42.9.691 1.501.691a2 2 0 10-1.955-1.582L5.501 6.691C5.134 6.27 4.6 6 4 6a2 2 0 100 4c.601 0 1.134-.27 1.5-.69l4.545 2.272A2 2 0 1012 10z" />
    </svg>
  );
};

ConnectionIcon.displayName = "ConnectionIcon";

export default ConnectionIcon;
