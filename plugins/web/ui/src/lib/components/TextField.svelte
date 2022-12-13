<script lang="ts">
  export let value: string;
  export let placeholder: string;
</script>

<div>
  <input
    value={value || ''}
    placeholder={placeholder || ''}
    on:click
    on:blur
    on:focus
    on:input
    on:keyup
    on:blur={onBlur}
    on:focus={onFocus}
    on:input={onInput}
    on:keyup={updateValueOnEnter}
    {type}
    class="spectrum-Textfield-input"
    style={align ? `text-align: ${align};` : ''}
    inputmode={type === 'number' ? 'decimal' : 'text'}
  />
</div>
