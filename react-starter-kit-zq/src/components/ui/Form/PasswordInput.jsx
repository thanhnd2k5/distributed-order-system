import PropTypes from "prop-types";
import { forwardRef, useState } from "react";
import { EyeIcon, EyeSlashIcon } from "@heroicons/react/24/outline";
import { Input } from "./Input";

const PasswordInput = forwardRef((props, ref) => {
  const {
    defaultVisible = false,
    visibilityToggle = true,
    suffix,
    ...rest
  } = props;
  const [visible, setVisible] = useState(defaultVisible);

  const toggleSuffix = (
    <button
      type="button"
      className="flex size-full items-center justify-center"
      onClick={() => setVisible((prev) => !prev)}
      aria-label={visible ? "Ẩn mật khẩu" : "Hiện mật khẩu"}
    >
      {visible ? (
        <EyeSlashIcon className="size-5" strokeWidth="1.5" />
      ) : (
        <EyeIcon className="size-5" strokeWidth="1.5" />
      )}
    </button>
  );

  return (
    <Input
      ref={ref}
      {...rest}
      type={visible ? "text" : "password"}
      suffix={visibilityToggle ? toggleSuffix : suffix}
    />
  );
});

PasswordInput.displayName = "PasswordInput";

PasswordInput.propTypes = {
  defaultVisible: PropTypes.bool,
  visibilityToggle: PropTypes.bool,
  suffix: PropTypes.node,
};

export { PasswordInput };
